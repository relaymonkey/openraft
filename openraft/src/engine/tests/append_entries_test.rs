use std::sync::Arc;
use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::Membership;
use crate::MembershipState;
use crate::Vote;
use crate::batch::Batch;
use crate::core::ServerState;
use crate::engine::Command;
use crate::engine::Condition;
use crate::engine::Engine;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::entry::RaftEntry;
use crate::errors::ConflictingLogId;
use crate::errors::RejectAppendEntries;
use crate::errors::RejectVote;
use crate::raft::LogSegment;
use crate::raft_state::IOId;
use crate::raft_state::LogStateReader;
use crate::testing::blank_ent;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::EntryOf;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;
use crate::vote::raft_vote::RaftVoteExt;

fn m01() -> Membership<u64, ()> {
    Membership::<u64, ()>::new_with_defaults(vec![btreeset! {0,1}], [])
}

fn m23() -> Membership<u64, ()> {
    Membership::<u64, ()>::new_with_defaults(vec![btreeset! {2,3}], [])
}

fn m34() -> Membership<u64, ()> {
    Membership::<u64, ()>::new_with_defaults(vec![btreeset! {3,4}], [])
}

fn eng() -> Engine<UTConfig> {
    let mut eng = Engine::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = 2;
    eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), Vote::new(2, 1));
    // Last-per-leader format: leader 1's last at index 2, leader 2's last at index 3
    eng.state.log_ids.append(log_id(1, 1, 2));
    eng.state.log_ids.append(log_id(2, 1, 3));
    eng.state.apply_progress_mut().accept(log_id(0, 1, 0));
    eng.state.membership_state = MembershipState::new(
        Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
        Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(2, 1, 3)), m23())),
    );
    eng.state.server_state = eng.calc_server_state();
    eng
}

#[test]
fn test_append_entries_vote_is_rejected() -> anyhow::Result<()> {
    let mut eng = eng();

    let res = eng.append_entries(&Vote::new(1, 1), LogSegment::new(None, vec![]));

    assert_eq!(
        Err(RejectAppendEntries::RejectVote(RejectVote {
            higher: Vote::new(2, 1),
        })),
        res
    );
    assert_eq!(None, eng.state.log_ids.purged());
    assert_eq!(
        &[
            log_id(1, 1, 2), // Last-per-leader: leader 1's last at index 2
            log_id(2, 1, 3),
        ],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(Vote::new(2, 1), *eng.state.vote_ref());
    assert_eq!(Some(&log_id(2, 1, 3)), eng.state.last_log_id());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(2, 1, 3)), m23())),
        ),
        eng.state.membership_state
    );
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(0, eng.output.take_commands().len());

    Ok(())
}

#[test]
fn test_append_entries_prev_log_id_is_applied() -> anyhow::Result<()> {
    // An applied log id has to be committed thus
    let mut eng = eng();
    eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), Vote::new(1, 2));
    eng.output.take_commands();

    let res = eng.append_entries(
        &Vote::new_committed(2, 1),
        LogSegment::new(Some(log_id(0, 1, 0)), vec![]),
    );

    assert_eq!(Ok(Some(log_id(0, 1, 0))), res);
    assert_eq!(None, eng.state.log_ids.purged());
    assert_eq!(
        &[
            log_id(1, 1, 2), // Last-per-leader: leader 1's last at index 2
            log_id(2, 1, 3), //
        ],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(Some(&log_id(2, 1, 3)), eng.state.last_log_id());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(2, 1, 3)), m23())),
        ),
        eng.state.membership_state
    );
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(
        vec![
            Command::FailPendingReads,
            Command::SaveVote {
                vote: Vote::new_committed(2, 1)
            },
            Command::UpdateIOProgress {
                when: Some(Condition::IOFlushed {
                    io_id: IOId::new_log_io(Vote::new(2, 1).to_committed(), None)
                }),
                io_id: IOId::new_log_io(Vote::new(2, 1).to_committed(), Some(log_id(0, 1, 0)))
            }
        ],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_append_entries_prev_log_id_conflict() -> anyhow::Result<()> {
    let mut eng = eng();

    let res = eng.append_entries(
        &Vote::new_committed(2, 1),
        LogSegment::new(Some(log_id(2, 1, 2)), vec![]),
    );

    assert_eq!(
        Err(RejectAppendEntries::ConflictingLogId(ConflictingLogId {
            expect: log_id(2, 1, 2),
            local: Some(log_id(1, 1, 2)),
        })),
        res
    );
    assert_eq!(None, eng.state.log_ids.purged());
    assert_eq!(
        &[
            log_id(1, 1, 1), //
        ],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(Some(&log_id(1, 1, 1)), eng.state.last_log_id());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
        ),
        eng.state.membership_state
    );
    assert_eq!(ServerState::Learner, eng.state.server_state);
    assert_eq!(
        vec![
            Command::FailPendingReads,
            Command::SaveVote {
                vote: Vote::new_committed(2, 1)
            },
            Command::TruncateLog {
                after: Some(log_id(1, 1, 1))
            },
        ],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_append_entries_prev_log_id_is_committed() -> anyhow::Result<()> {
    let mut eng = eng();

    let res = eng.append_entries(
        &Vote::new_committed(2, 1),
        LogSegment::new(Some(log_id(0, 1, 0)), vec![
            blank_ent::<UTConfig>(1, 1, 1),
            blank_ent::<UTConfig>(2, 1, 2),
        ]),
    );

    assert_eq!(Ok(Some(log_id(2, 1, 2))), res);
    assert_eq!(None, eng.state.log_ids.purged());
    assert_eq!(
        &[
            log_id(1, 1, 1), // After truncation: leader 1 ends at index 1
            log_id(2, 1, 2), // New leader 2 entry at index 2
        ],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(Some(&log_id(2, 1, 2)), eng.state.last_log_id());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
        ),
        eng.state.membership_state
    );
    assert_eq!(ServerState::Learner, eng.state.server_state);
    assert_eq!(
        vec![
            Command::FailPendingReads,
            Command::SaveVote {
                vote: Vote::new_committed(2, 1)
            },
            Command::TruncateLog {
                after: Some(log_id(1, 1, 1))
            },
            Command::AppendEntries {
                committed_vote: Vote::new(2, 1).to_committed(),
                entries: Batch::of([blank_ent::<UTConfig>(2, 1, 2)])
            },
        ],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_append_entries_prev_log_id_not_exists() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), Vote::new(1, 2));
    eng.output.take_commands();

    let res = eng.append_entries(
        &Vote::new_committed(2, 1),
        LogSegment::new(Some(log_id(2, 1, 4)), vec![
            blank_ent::<UTConfig>(2, 1, 5),
            blank_ent::<UTConfig>(2, 1, 6),
        ]),
    );

    assert_eq!(
        Err(RejectAppendEntries::ConflictingLogId(ConflictingLogId {
            expect: log_id(2, 1, 4),
            local: None,
        })),
        res
    );
    assert_eq!(None, eng.state.log_ids.purged());
    assert_eq!(
        &[
            log_id(1, 1, 2), // Last-per-leader: leader 1's last at index 2
            log_id(2, 1, 3), //
        ],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(Some(&log_id(2, 1, 3)), eng.state.last_log_id());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(2, 1, 3)), m23())),
        ),
        eng.state.membership_state
    );
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert_eq!(
        vec![Command::FailPendingReads, Command::SaveVote {
            vote: Vote::new_committed(2, 1)
        },],
        eng.output.take_commands()
    );

    Ok(())
}

#[test]
fn test_append_entries_conflict() -> anyhow::Result<()> {
    // prev_log_id matches,
    // The second entry in entries conflict.
    // This request will replace the effective membership.
    // committed is greater than entries.
    // It is no longer a member, change to learner
    let mut eng = eng();

    let resp = eng.append_entries(
        &Vote::new_committed(2, 1),
        LogSegment::new(Some(log_id(1, 1, 1)), vec![
            blank_ent::<UTConfig>(1, 1, 2),
            EntryOf::<UTConfig>::new_membership(log_id(3, 1, 3), m34()),
        ]),
    );

    assert_eq!(Ok(Some(log_id(3, 1, 3))), resp);
    assert_eq!(None, eng.state.log_ids.purged());
    assert_eq!(
        &[
            log_id(1, 1, 2), // Leader 1's last is at index 2 (blank_ent at index 2)
            log_id(3, 1, 3), // Leader 3's last is at index 3 (membership entry)
        ],
        eng.state.log_ids.key_log_ids()
    );
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(Some(&log_id(3, 1, 3)), eng.state.last_log_id());
    assert_eq!(
        MembershipState::new(
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(1, 1, 1)), m01())),
            Arc::new(StoredMembershipOf::<UTConfig>::new(Some(log_id(3, 1, 3)), m34())),
        ),
        eng.state.membership_state
    );
    assert_eq!(ServerState::Learner, eng.state.server_state);
    assert_eq!(
        vec![
            Command::FailPendingReads,
            Command::SaveVote {
                vote: Vote::new_committed(2, 1)
            },
            Command::TruncateLog {
                after: Some(log_id(1, 1, 2))
            },
            Command::AppendEntries {
                committed_vote: Vote::new(2, 1).to_committed(),
                entries: Batch::of([EntryOf::<UTConfig>::new_membership(log_id(3, 1, 3), m34())])
            },
        ],
        eng.output.take_commands()
    );

    Ok(())
}

/// The `when` condition of the single `Respond` command the engine queued.
fn respond_condition(eng: &mut Engine<UTConfig>) -> Option<Condition<UTConfig>> {
    let mut found = None;
    for cmd in eng.output.take_commands() {
        if let Command::Respond { when, .. } = cmd {
            assert!(found.is_none(), "one Respond per append");
            found = Some(when);
        }
    }
    found.expect("a Respond command")
}

fn leader_vote() -> crate::type_config::alias::VoteOf<UTConfig> {
    Vote::new_committed(2, 1)
}

fn flushed(last: Option<crate::type_config::alias::LogIdOf<UTConfig>>) -> Option<Condition<UTConfig>> {
    Some(Condition::IOFlushed {
        io_id: IOId::new_log_io(leader_vote().to_committed(), last),
    })
}

#[test]
fn test_handle_append_entries_empty_waits_for_prev_log_id_not_for_accepted_entries() -> anyhow::Result<()> {
    // An append without entries is answered once the leader's vote and its `prev_log_id` are flushed,
    // although a bulk append accepted just before it is still in flight.
    let mut eng = eng();
    eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), leader_vote());
    eng.output.take_commands();

    // The bulk append: accepted, answered only after its own entries flush.
    let (tx, _rx) = UTConfig::<()>::oneshot();
    eng.handle_append_entries(
        &leader_vote(),
        LogSegment::new(Some(log_id(2, 1, 3)), vec![
            blank_ent::<UTConfig>(2, 1, 4),
            blank_ent::<UTConfig>(2, 1, 5),
        ]),
        tx,
    );
    assert_eq!(flushed(Some(log_id(2, 1, 5))), respond_condition(&mut eng));

    // The heartbeat: `prev_log_id` 3 is not the accepted 5.
    let (tx, _rx) = UTConfig::<()>::oneshot();
    eng.handle_append_entries(&leader_vote(), LogSegment::new(Some(log_id(2, 1, 3)), vec![]), tx);
    assert_eq!(
        flushed(Some(log_id(2, 1, 3))),
        respond_condition(&mut eng),
        "an empty append does not wait for the accepted entries"
    );

    // An empty append at the accepted tail waits for the tail.
    let (tx, _rx) = UTConfig::<()>::oneshot();
    eng.handle_append_entries(&leader_vote(), LogSegment::new(Some(log_id(2, 1, 5)), vec![]), tx);
    assert_eq!(flushed(Some(log_id(2, 1, 5))), respond_condition(&mut eng));

    // An empty append at the very start of the log waits for the vote alone.
    let (tx, _rx) = UTConfig::<()>::oneshot();
    eng.handle_append_entries(&leader_vote(), LogSegment::new(None, vec![]), tx);
    assert_eq!(flushed(None), respond_condition(&mut eng));

    Ok(())
}

#[test]
fn test_handle_append_entries_empty_that_raises_the_vote_waits_for_the_vote() -> anyhow::Result<()> {
    // The node holds only an uncommitted vote; the empty append carries the leader's committed vote.
    // The condition's vote is the leader's, so it is met only after that vote's save has flushed.
    let mut eng = eng();
    eng.output.take_commands();

    let (tx, _rx) = UTConfig::<()>::oneshot();
    eng.handle_append_entries(&leader_vote(), LogSegment::new(Some(log_id(1, 1, 2)), vec![]), tx);

    let cmds = eng.output.take_commands();
    assert!(
        cmds.iter().any(|c| matches!(c, Command::SaveVote { .. })),
        "the vote is saved"
    );
    let when = cmds.into_iter().find_map(|c| match c {
        Command::Respond { when, .. } => Some(when),
        _ => None,
    });
    assert_eq!(Some(flushed(Some(log_id(1, 1, 2)))), when);

    Ok(())
}

#[test]
fn test_handle_append_entries_with_entries_waits_for_the_accepted_io() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.state.vote = Leased::new(UTConfig::<()>::now(), Duration::from_millis(500), leader_vote());
    eng.output.take_commands();

    let (tx, _rx) = UTConfig::<()>::oneshot();
    eng.handle_append_entries(
        &leader_vote(),
        LogSegment::new(Some(log_id(2, 1, 3)), vec![blank_ent::<UTConfig>(2, 1, 4)]),
        tx,
    );
    assert_eq!(flushed(Some(log_id(2, 1, 4))), respond_condition(&mut eng));

    Ok(())
}

#[test]
fn test_handle_append_entries_rejected_has_no_condition() -> anyhow::Result<()> {
    let mut eng = eng();
    eng.output.take_commands();

    let (tx, _rx) = UTConfig::<()>::oneshot();
    eng.handle_append_entries(&Vote::new(1, 1), LogSegment::new(None, vec![]), tx);
    assert_eq!(None, respond_condition(&mut eng));

    Ok(())
}
