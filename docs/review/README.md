# Review material for pull request #412

This directory is the reading material for
[pull request #412](https://github.com/NodeDB-Lab/nodedb/pull/412), which makes the startup bounds
configurable and makes NodeDB say which WAL format version it found when it refuses a store. It lives on
its own branch so the pull request's diff stays code only.

| File | What it is |
| --- | --- |
| [`issue354-review-packet.md`](issue354-review-packet.md) | The review packet. The five commits, the evidence with exit codes, the design decisions to check, and the questions to answer. Start here |
| [`boot-cost-findings.md`](boot-cost-findings.md) | What the boot spends and why, measured: the per-vshard WAL replay, the fat-increment experiments, the two-machine hardware comparison |
| [`boot-cost-flexibility-design.md`](boot-cost-flexibility-design.md) | Where flexibility pays and where it does not, each row tied to a measurement, plus the rule that where the system's own numbers move, a fixed value will be wrong sometimes |
| [`plan-next-work.md`](plan-next-work.md) | The plan for the work after this pull request, one branch and one pull request, each step with the code-graph survey that would contradict it |
| [`mutation-version-read-removed.diff`](mutation-version-read-removed.diff) | The exact code removed in the mutation run, so the assertion strength is auditable rather than asserted |

## The change in one paragraph

A node with a long WAL tail spends nearly all of its boot replaying the same WAL once per hosted
vshard, which on the store that motivated this work is 83 s of a 90 s boot. The bounds meant to catch a
stuck boot were hard-coded and do not cover the phase that dominates. Separately, a store written in an
older WAL format failed every boot with "the segment appears to be corrupted", which sent its reader
looking for damage that was not there. It now says which format version it found and which this build
requires.

## Evidence at a glance

| Arm | Exit | What it shows |
| --- | --- | --- |
| red | 100 | The version test fails against the old wording |
| green | 0 | The replay module and the error-class parity module pass |
| mutation | 100 | With the version read removed, exactly the two version-message tests fail |
| end to end | 0 | A real version-1 production copy is refused naming both versions, and a real version-3 store boots to serving |
| full suite | 0 | 8,784 tests passed on the head |
