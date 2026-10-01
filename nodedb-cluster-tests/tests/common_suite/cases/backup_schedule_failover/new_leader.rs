// SPDX-License-Identifier: BUSL-1.1

//! A new vShard 0 leader runs the due minute the dead leader never fired.

use nodedb::control::backup::schedule::envelope_name;

use super::fixture::{DATABASE, Fixture};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_vshard0_leader_runs_the_due_minute_exactly_once() {
    let mut fx = Fixture::new().await;
    let due = fx.due;

    // One minute before `due`, every node ticks. Only the leader arms.
    fx.tick_all((due - 1) * 60 + 5).await;
    fx.wait_marks("every node holds the armed mark", due - 1)
        .await;

    // `due` comes due, and the leader dies before its tick fires it.
    fx.kill_leader().await;

    // The survivors tick after `due`, until the mark reaches it. A tick that
    // fails waits out its retry delay on the scheduler clock, so each
    // attempt moves the clock past it. No minute from `due + 1` through
    // `due + 4` matches, so every attempt runs `due` itself.
    let mut now_secs = (due + 1) * 60 + 5;
    for _ in 0..3 {
        fx.tick_all(now_secs).await;
        if fx.local_marks().contains(&Some(due)) {
            break;
        }
        now_secs += 61;
    }
    fx.wait_marks("every survivor holds the mark of the due minute", due)
        .await;

    // Later ticks find nothing due.
    fx.tick_all((due + 4) * 60 + 30).await;

    // Exactly one run of `due`, on the new leader, and one envelope for it.
    let new_leader = fx.leader();
    let mut successes = 0;
    for node in &fx.cluster.nodes {
        let (ok, failed) = fx.runs(node);
        if node.node_id == new_leader {
            assert_eq!(
                ok, 1,
                "the new leader runs the due minute once ({failed} failed)"
            );
        } else {
            assert_eq!(
                (ok, failed),
                (0, 0),
                "node {} is no leader and runs nothing",
                node.node_id
            );
        }
        successes += ok;
    }
    assert_eq!(successes, 1);
    assert_eq!(fx.envelopes(), [envelope_name(DATABASE, due * 60_000)]);

    fx.shutdown().await;
}
