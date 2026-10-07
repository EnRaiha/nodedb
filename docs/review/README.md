# Evidence for pull request #412

Reading material and raw logs for [NodeDB-Lab/nodedb#412](https://github.com/NodeDB-Lab/nodedb/pull/412),
which makes the startup bounds configurable and makes startup name the WAL format version it found when
it refuses a store. It lives on its own branch so the pull request's diff stays code only.

## The evidence, arm by arm

Every log below is the unedited output of the command beside it, with paths and hostnames replaced.
Exit codes are the process exit code: 0 passes, 100 is a test failure, 101 is a build failure.

| Arm | Command | Exit | Commit | Log | sha256 |
| --- | --- | --- | --- | --- | --- |
| red | `cargo nextest run -p nodedb --lib -E 'test(a_store_written_in_an_older_format_says_which_version)'` | 100 | `e01612439` | [`logs/red-base.log`](logs/red-base.log) | `458af8cbab467489` |
| green | `cargo nextest run -p nodedb --lib -E 'test(/wal::manager::replay/) or test(/class_parity/)'` and `cargo nextest run -p nodedb-wal -E 'test(/segmented/)'` | 0 | `7ed16c1f0` | [`logs/green-7ed16c1f0.log`](logs/green-7ed16c1f0.log) | `b2fb3a7c8663a493` |
| mutation A | the same filter, reader's version error removed | 100 | `afe1c835e` | [`logs/mutation-A-reader-surfacing.log`](logs/mutation-A-reader-surfacing.log) | `c0f9bbcfbbba6d2b` |
| mutation B | the same filter, version-zero guard removed | 100 | `afe1c835e` | [`logs/mutation-B-zeroed-guard.log`](logs/mutation-B-zeroed-guard.log) | `ff1cfeb1452404ab` |
| mutation C | the same filter, the reader's version-zero arm removed | 100 | `7ed16c1f0` | [`logs/mutation-zeroed-guard.log`](logs/mutation-zeroed-guard.log) | `6413fe585cd2a87e` |
| end to end, message reaches the operator | `scripts/e2e-version-message.sh`, a real production copy and a real version-3 store | 0 | `3ec504dfb` | [`logs/e2e-message-reaches-operator.log`](logs/e2e-message-reaches-operator.log) | `60c3394d3cbf4cc9` |
| end to end, first version | the same script, before the boot-order fix | 0 | `435fb0b42` | [`logs/e2e-version-message.log`](logs/e2e-version-message.log) | `d0053996e876bb98` |
| full suite | `cargo nextest run -p nodedb --lib` | 0 | `d23e47984` | [`logs/full-suite.log`](logs/full-suite.log) | `bb60f23ed6599889` |

## Documents

| File | What it answers |
| --- | --- |
| [`issue354-review-packet.md`](issue354-review-packet.md) | The problem, the root cause, the design decisions, the evidence, and the questions for a reviewer. Start here |
| [`boot-cost-findings.md`](boot-cost-findings.md) | What a boot spends and why, measured on real stores: the per-vshard WAL replay, the fat-increment experiments, and the same store on a 2023 laptop and a 2018 desktop |
| [`boot-cost-flexibility-design.md`](boot-cost-flexibility-design.md) | Where a knob changes an outcome and where it would buy nothing, each row tied to a measurement |
| [`plan-next-work.md`](plan-next-work.md) | The plan for the work after this pull request, each step with the code-graph survey that would contradict it |
| [`scripts/e2e-version-message.sh`](scripts/e2e-version-message.sh) | The end-to-end script: boots a real store and asserts what the refusal says |
| [`mutation/mutation-version-read-removed.diff`](mutation/mutation-version-read-removed.diff) | The first attempt's mutation diff, kept as the record of what that arm removed |

## What is not here, on purpose

| Item | Why |
| --- | --- |
| The production store itself | It holds real data. Its size and digest are recorded in the packet instead. A segment near 60 MB also sits close to a hosting limit |
| Customer, tenant and host names | Replaced with `<user>`, `<host>`, `<ip>`, `<store>` in every log and document |
| Anything from a private wiki or a local ledger | Those paths were rewritten, so every reference here resolves inside this branch |

## Tags

Each tested head is tagged, so a log and the code it came from can be checked out together.
