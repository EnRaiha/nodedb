# Security Policy

NodeDB is a **pre-1.0 public beta**. It is not yet recommended for production use, and interfaces, defaults, and on-disk formats may change between releases. We still take security seriously and investigate every good-faith report — this policy explains what qualifies, how to report it, and how we respond.

Security is a shared responsibility. This policy covers vulnerabilities in NodeDB itself. The security of a deployment also depends on its environment — the host, operating system, network boundary, and the application and client libraries built on top of NodeDB. See the operator guide for how to configure and run NodeDB securely.

## Supported Versions

While NodeDB is pre-1.0, security fixes are issued **only against the latest released minor version**. Once 1.0 ships, this policy will be updated to define an LTS window.

| Version  | Supported          |
| -------- | ------------------ |
| 0.5.x    | :white_check_mark: |
| <= 0.4.x | :x:                |

Always run the latest release; it contains every prior security fix along with other bug fixes.

## What Qualifies as a Security Vulnerability

A security vulnerability in NodeDB is an issue that lets a user reach privileges or data they were not granted, or execute arbitrary code through a NodeDB process. Concretely, in scope as vulnerabilities:

- Authentication bypass, or privilege escalation to superuser by an unprivileged user.
- Cross-tenant or cross-database access beyond what a principal was granted.
- Bypass of a documented authorization gate (`GRANT`, row-level security, `_system` catalog restriction) on **any** transport (pgwire, HTTP, WebSocket RPC, native, ILP, CRDT sync).
- Reading or writing data as an identity you do not control (e.g. trusting a client-supplied claim, header, or token field for a security decision).
- Memory-safety issues or arbitrary code execution reachable from the wire.

Generally **not** treated as vulnerabilities (report these as ordinary bugs instead — they are still worth fixing):

- Actions a legitimately-granted **superuser** can take. Superuser is all-powerful by design.
- Denial-of-service caused by an authenticated principal issuing an expensive but valid query on a deployment without configured limits. Configure memory governors, query timeouts, and per-tenant budgets per the operator guide. (A wire-reachable panic or crash from _malformed_ input, by contrast, is in scope.)
- Vulnerabilities in third-party dependencies that are already tracked by their own advisory. Please still tell us so we can upgrade.
- Issues that require physical access to an already-compromised host, or issues in development tooling (`scripts/`, benchmarks, examples) that do not affect the shipped binary.

If you are unsure whether something qualifies, err on the side of reporting it privately.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security reports.**

Report privately via GitHub's **Security Advisories** workflow:

> [Report a vulnerability](https://github.com/NodeDB-Lab/nodedb/security/advisories/new)

Or go to the repository's **Security** tab → **Advisories** → **Report a vulnerability**. This opens a private channel between you and the maintainers; nothing is public until an advisory is published.

Please include:

- **Overview** — a clear description of the issue and its security impact.
- **Affected component and version** — the output of `nodedb --version`, the deployment mode (Origin cloud, Origin local, or Lite), and the affected component (engine, transport, WAL/segment format, cluster, sync).
- **Reproduction** — step-by-step instructions or a minimal proof-of-concept.
- **Environment** — OS, and any relevant configuration.
- **Evidence** — proof-of-concept code, log excerpts, stack traces, or a patch if you have one.
- Whether the issue is already public or under coordinated disclosure elsewhere (e.g. a RustSec advisory on a dependency).

## Our Response

When you report an issue we will:

1. **Acknowledge** receipt within **3 business days**.
2. Provide an **initial assessment** (confirmed / needs more info / not a vulnerability, with severity and scope) within **10 business days**.
3. Keep you **updated at least every 14 days** while we work on a fix.
4. Prepare and review the fix, cut a release on the latest minor line, and **publish a GitHub Security Advisory once the fixed release is available** — unless the issue is already public.

We coordinate disclosure: the reporter and maintainers agree on an embargo (typically up to 90 days, shorter for actively-exploited issues), and we ask that the vulnerability not be disclosed publicly until a fixed release is out.

## CVEs

Advisories are published through GitHub Security Advisories. GitHub acts as the CVE Numbering Authority; we request a CVE when an issue warrants one. While NodeDB is a pre-1.0 beta, many advisories are published with a GHSA identifier only. Please **do not register a CVE independently** — coordinate it with us through the advisory so the record stays accurate.

The published advisories, with affected and fixed versions, CVSS scores, and whether a valid login is required, are listed on the repository's [Security Advisories](https://github.com/NodeDB-Lab/nodedb/security/advisories) page.

We do not currently offer a bug bounty.

## Credit and Safe Harbor

We credit reporters in the published advisory and release notes by default. If you prefer to remain anonymous or to use a specific handle, say so in your report.

We consider security research conducted in good faith under this policy to be authorized, and we will not pursue legal action against you for it. In return, we ask that you:

- Give us reasonable time to fix the issue before any public disclosure.
- Do not access, modify, or exfiltrate data that is not yours; use only the minimum access needed to demonstrate the issue.
- Do not run denial-of-service tests or automated scanners against infrastructure you do not own without explicit permission.

## Scope

In scope:

- The `nodedb` server binary and all crates in this repository.
- The published `nodedb-*` crates on crates.io.
- The pgwire, HTTP (JSON and NDJSON), WebSocket RPC, native MessagePack, and ILP protocols.
- The CRDT sync protocol between Origin and Lite.
- The on-disk WAL, segment, and snapshot formats.

Out of scope (report to the relevant project or as a normal bug):

- The separate `nodedb-lite`, `nodedb-cli`, and `nodedb-studio` repositories — report there, or here if you are unsure.
- Third-party dependencies already tracked by their own advisory (tell us so we can upgrade).
- The non-vulnerability categories listed under _What Qualifies_ above.

## Security Automation

- Dependencies are checked for known advisories in CI with **`cargo deny check`** (RustSec advisory database, license, and source-ban policy). Vulnerable dependencies must be upgraded, replaced, or explicitly acknowledged during review.
- Clippy runs workspace-wide with warnings denied.

## Hardening Defaults

Deployments handling real data should:

- Enable TLS on the pgwire listener (`pgwire.tls.cert` / `pgwire.tls.key`).
- Use a non-trust authentication method (`pgwire.auth = "scram-sha-256"`).
- Enable WAL encryption (`wal.encryption = "aes-256-gcm"`) on untrusted storage.
- Configure per-tenant memory and IO budgets via the `nodedb-mem` governors.
- Restrict the cluster QUIC listener to a private network.
- Never expose an unauthenticated ILP listener; bind it to an authenticated principal or a trusted network only.
