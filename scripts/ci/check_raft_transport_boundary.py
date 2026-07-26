#!/usr/bin/env python3
"""Keep the consensus crate network-agnostic and behind cluster authentication."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAFT = ROOT / "nodedb-raft"

FORBIDDEN_SOURCE = {
    "quinn::": "direct QUIC access",
    "nexar::": "direct cluster transport access",
    "TcpListener": "direct TCP listener",
    "UdpSocket": "direct UDP socket",
    "TcpStream": "direct TCP stream",
}
FORBIDDEN_DEPS = {"quinn", "nexar", "rustls", "tokio-rustls"}
ALLOWED_IMPL = ROOT / "nodedb-cluster/src/transport/client/raft_impl.rs"


def main() -> int:
    errors: list[str] = []
    cargo = (RAFT / "Cargo.toml").read_text(encoding="utf-8")
    for dependency in sorted(FORBIDDEN_DEPS):
        if re.search(rf"(?m)^\s*{re.escape(dependency)}\s*=", cargo):
            errors.append(f"nodedb-raft/Cargo.toml: forbidden network dependency {dependency}")

    for source in sorted((RAFT / "src").rglob("*.rs")):
        text = source.read_text(encoding="utf-8")
        for token, reason in FORBIDDEN_SOURCE.items():
            if token in text:
                errors.append(f"{source.relative_to(ROOT)}: {reason} ({token})")

    impl_pattern = re.compile(r"\bimpl(?:\s*<[^{}]*>)?\s+RaftTransport\s+for\b")
    for source in sorted(ROOT.rglob("*.rs")):
        if any(part in {"target", ".git"} for part in source.parts):
            continue
        if impl_pattern.search(source.read_text(encoding="utf-8")) and source != ALLOWED_IMPL:
            errors.append(
                f"{source.relative_to(ROOT)}: RaftTransport implementation must live at "
                f"{ALLOWED_IMPL.relative_to(ROOT)}"
            )

    if errors:
        print("Raft transport boundary violations:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print("OK: Raft consensus remains behind the authenticated cluster transport.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
