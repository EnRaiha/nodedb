# Evidence for pull request #412

Reading material and raw logs for [NodeDB-Lab/nodedb#412](https://github.com/NodeDB-Lab/nodedb/pull/412),
which makes the startup bounds configurable and makes startup name the WAL format version it found when
it refuses a store. It lives on its own branch so the pull request's diff stays code only.

## The evidence, arm by arm

Every log below is the unedited output of the command beside it, with paths and hostnames replaced.
Exit codes are the process exit code: 0 passes, 100 is a test failure, 101 is a build failure.

| Arm | Command | Exit | Commit on the branch | Log | sha256 |
| --- | --- | --- | --- | --- | --- |
| green | the replay and class-parity modules, the reader tests, the segmented writer tests | 0 | `c620167ca` | [`logs/green-c620167ca.log`](logs/green-c620167ca.log) | `67e51ce6008a7b1a` |
| mutation | the same, with the reader's version error removed | 100 | `c620167ca` | [`logs/mutation-reader-version.log`](logs/mutation-reader-version.log) | `f8e4b24fae7e084b` |
| end to end | a real production copy and a real version-3 store, release binary | 0 | `c620167ca` | [`logs/e2e-c620167ca.log`](logs/e2e-c620167ca.log) | `800a00ce17a92159` |
| full suite | `cargo nextest run -p nodedb --lib` | 0 | `c620167ca` | [`logs/full-suite.log`](logs/full-suite.log) | `e9648d9ae143b14b` |
| preflight, clippy included | the repository preflight, full mode against the base | 0 | `c620167ca` | [`logs/preflight.log`](logs/preflight.log) | `223a9fb8f6169e0c` |

The red proof for this change is the mutation arm: the same tests fail when the reader's version error is removed, which is the state before the fix. The base run recorded earlier was on a commit that the history rewrite replaced, so it is not cited here.

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

## Verdict

Review 2: **PASS, 0 blockers**, bound to head `c620167ca6e50ac9093526eb4b5f96b165a6dcb2`.

The full library suite is **8,785 tests, 0 failures** on this head. The branch is five commits: the startup bounds, the WAL diagnosis, two clippy fixes, and two commits for the reader test. A maintainer rebase-merging can fold the last two.

Each tested head is tagged, and the tag points at the commit carrying its logs: `pr412-b0f1c8f3` for the three-commit head, `pr412-c620167c` for this one.
