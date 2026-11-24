use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::Membership;
use crate::MembershipState;
use crate::Vote;
use crate::core::ServerState;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;

fn m1() -> Membership<u64, ()> {
    Membership::new_with_defaults(vec![btreeset! {1}], [])
}

fn m12() -> Membership<u64, ()> {
    Membership::new_with_defaults(vec![btreeset! {1,2}], [])
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::testing_default(0);
    eng.state.log_ids = LogIdList::new(None, [log_id(0, 0, 0)]);
    eng.state.enable_validation(false); // Disable validation for incomplete state
    eng
}

#[test]
fn test_pre_elect_multi_node_no_mutation() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.config.id = 1;
    eng.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
        Some(log_id(0, 1, 1)),
        m12(),
    )));
    eng.state.log_ids = LogIdList::new(None, vec![log_id(1, 1, 1)]);

    let vote_before = *eng.state.vote_ref();
    let server_state_before = eng.state.server_state;

    eng.pre_elect();

    // A pre-vote must NOT bump the term, persist a vote, or change server state.
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(server_state_before, eng.state.server_state);
    assert!(eng.candidate_ref().is_none(), "no real candidate during pre-vote");

    // A pre-candidate at the hypothetical next term, having granted itself.
    assert_eq!(Vote::new(1, 1), *eng.pre_candidate_ref().unwrap().vote_ref());
    assert_eq!(
        btreeset! {1},
        eng.pre_candidate_ref().unwrap().granters().collect::<BTreeSet<_>>()
    );

    // Only SendPreVote is emitted — crucially, NO SaveVote and NO SendVote.
    assert_eq!(
        vec![Command::SendPreVote {
            vote_req: VoteRequest::new(Vote::new(1, 1), Some(log_id(1, 1, 1)))
        }],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_pre_elect_refused_by_valid_leader_lease() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.config.id = 1;
    eng.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
        Some(log_id(0, 1, 1)),
        m12(),
    )));
    eng.state.log_ids = LogIdList::new(None, vec![log_id(1, 1, 1)]);
    // A follower served by a live Leader: a committed vote whose lease has not expired.
    eng.state.vote = Leased::new(
        UTConfig::<()>::now(),
        Duration::from_millis(500),
        Vote::new_committed(1, 2),
    );

    let timeout_before = eng.config.timer_config.election_timeout;
    let vote_before = *eng.state.vote_ref();

    eng.pre_elect();

    // Refused before touching anything: no new timeout sample, no pre-candidate, no command.
    assert_eq!(timeout_before, eng.config.timer_config.election_timeout);
    assert!(eng.pre_candidate_ref().is_none());
    assert!(eng.candidate_ref().is_none());
    assert_eq!(vote_before, *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_pre_elect_single_node_starts_real_election() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.config.id = 1;
    eng.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
        Some(log_id(0, 1, 1)),
        m1(),
    )));

    eng.pre_elect();

    // A single voter wins its own pre-vote and proceeds straight to a real election.
    assert_eq!(Vote::new(1, 1), *eng.state.vote_ref());
    assert!(eng.candidate_ref().is_some());
    assert!(
        eng.pre_candidate_ref().is_none(),
        "pre-vote consumed by the real election"
    );
    assert_eq!(ServerState::Candidate, eng.state.server_state);

    assert_eq!(
        vec![Command::SaveVote { vote: Vote::new(1, 1) }, Command::SendVote {
            vote_req: VoteRequest::new(Vote::new(1, 1), Some(log_id(0, 0, 0))),
        },],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_pre_elect_removed_committed_voter_uses_effective_quorum() {
    let mut eng = eng();
    eng.config.id = 1;
    eng.state.log_ids = LogIdList::new(None, [log_id(1, 0, 2)]);
    eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::ZERO, Vote::new(1, 0));
    eng.state.membership_state = MembershipState::new(
        Arc::new(StoredMembershipOf::<UTConfig>::new(
            Some(log_id(1, 0, 1)),
            Membership::new_with_defaults(vec![btreeset! {1,2,3}], []),
        )),
        Arc::new(StoredMembershipOf::<UTConfig>::new(
            Some(log_id(1, 0, 2)),
            Membership::new_with_defaults(vec![btreeset! {2,3}], []),
        )),
    );
    eng.state.update_local_committed(&Some(log_id(1, 0, 1)));

    tracing::info!("--- removed node 1 probes effective voters 2,3 without counting itself");
    {
        eng.pre_elect();

        assert_eq!(&Vote::new(1, 0), eng.state.vote_ref());
        assert!(eng.pre_candidate_ref().unwrap().granters().next().is_none());
        assert!(eng.candidate_ref().is_none());
        assert_eq!(
            vec![Command::SendPreVote {
                vote_req: VoteRequest::new(Vote::new(2, 1), Some(log_id(1, 0, 2))),
            }],
            eng.output.take_commands()
        );
    }

    tracing::info!("--- both effective voters must pre-vote before the term advances");
    {
        eng.handle_pre_vote_resp(2, VoteResponse::new(Vote::new(1, 0), Some(log_id(1, 0, 2)), true));

        assert_eq!(&Vote::new(1, 0), eng.state.vote_ref());
        assert!(eng.candidate_ref().is_none());

        eng.handle_pre_vote_resp(3, VoteResponse::new(Vote::new(1, 0), Some(log_id(1, 0, 2)), true));

        assert_eq!(&Vote::new(2, 1), eng.state.vote_ref());
        assert!(eng.pre_candidate_ref().is_none());
        assert!(eng.candidate_ref().is_some());
        assert_eq!(ServerState::Candidate, eng.state.server_state);
    }
}

/// A voter 1 in {1,2,3} that follows nobody, at vote term 0.
fn eng_voter_of_three() -> Engine<UTConfig> {
    let mut eng = eng();
    eng.config.id = 1;
    eng.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
        Some(log_id(0, 1, 1)),
        Membership::new_with_defaults(vec![btreeset! {1,2,3}], []),
    )));
    eng.state.log_ids = LogIdList::new(None, vec![log_id(1, 1, 1)]);
    eng
}

#[test]
fn test_pre_elect_at_runs_at_the_pinned_term() -> anyhow::Result<()> {
    let mut eng = eng_voter_of_three();

    eng.pre_elect_at(0);

    assert_eq!(Vote::new(1, 1), *eng.pre_candidate_ref().unwrap().vote_ref());
    assert_eq!(
        vec![Command::SendPreVote {
            vote_req: VoteRequest::new(Vote::new(1, 1), Some(log_id(1, 1, 1)))
        }],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_pre_elect_at_is_a_noop_when_the_term_moved() -> anyhow::Result<()> {
    let mut eng = eng_voter_of_three();
    eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::ZERO, Vote::new(3, 2));

    eng.pre_elect_at(2);

    assert!(eng.pre_candidate_ref().is_none());
    assert!(eng.candidate_ref().is_none());
    assert_eq!(Vote::new(3, 2), *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_pre_elect_at_queued_twice_raises_the_term_once() -> anyhow::Result<()> {
    // Two triggers pinned to term 0 are queued, the second before the first round's replies are
    // processed. Replies of round one arrive between them on the core: the second trigger then runs
    // from a Candidate at term 1 and must not ask for term 2.
    let mut eng = eng_voter_of_three();

    tracing::info!("--- trigger 1 starts the pre-vote round for term 1");
    eng.pre_elect_at(0);
    eng.output.take_commands();

    tracing::info!("--- the round is granted: the real election for term 1 starts");
    eng.handle_pre_vote_resp(2, VoteResponse::new(Vote::new(0, 0), Some(log_id(1, 1, 1)), true));
    assert_eq!(Vote::new(1, 1), *eng.state.vote_ref());
    assert!(eng.candidate_ref().is_some());
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    eng.output.take_commands();

    tracing::info!("--- trigger 2, still pinned to term 0, is suppressed");
    eng.pre_elect_at(0);
    assert!(eng.pre_candidate_ref().is_none());
    assert_eq!(Vote::new(1, 1), *eng.state.vote_ref());
    assert_eq!(0, eng.output.take_commands().len());

    tracing::info!("--- a trigger pinned to the candidate's own term is suppressed too");
    eng.pre_elect_at(1);
    assert!(eng.pre_candidate_ref().is_none());
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_pre_elect_at_second_trigger_before_the_first_is_granted_restarts_the_round() -> anyhow::Result<()> {
    // The residual: a trigger processed while round one is in flight passes the pin (term unchanged,
    // not a Candidate) and restarts the round at the same term. The term is not raised twice.
    let mut eng = eng_voter_of_three();

    eng.pre_elect_at(0);
    eng.pre_elect_at(0);

    assert_eq!(Vote::new(1, 1), *eng.pre_candidate_ref().unwrap().vote_ref());
    assert_eq!(Vote::new(0, 0), *eng.state.vote_ref());
    assert!(eng.candidate_ref().is_none());

    Ok(())
}
