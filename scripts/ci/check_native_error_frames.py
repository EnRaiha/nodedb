#!/usr/bin/env python3
"""Reject unclassified native error frames.

A native error frame carries a SQLSTATE *and* the stable numeric NodeDB code.
`NativeResponse::error` sets that number to `0`, which the client reads as a
generic internal failure — so `is_not_found()`, `is_auth_denied()` and
`is_retriable()` all answer wrongly for such a frame, and a retry loop stops
retrying a conflict it should retry.

Every site under `control/server/native/` must therefore build its frame
through one of the classifying constructors instead:

  - `error_to_native` / `error_to_native_with_sqlstate` — the site holds a
    classified `crate::Error`; the number comes from that error.
  - `sqlstate_error` — the site only ever had a SQLSTATE (a `DdlError`, a
    session guard); the number comes from the SQLSTATE table.
  - `NativeResponse::error_with_code` — the site already knows the number.

This gate exists because clearing these sites once does not keep them clear.
Fifty-three of them were swept in one change; nothing but this check stops the
fifty-fourth from being written, and a frame that regresses is invisible at the
call site — it looks like an ordinary error return and only misbehaves two
processes away.

`control/server/ilp_auth.rs` is deliberately NOT covered: it collapses every
authentication failure into one code and one static message so a caller cannot
tell a wrong password from an unknown user. It sits outside this prefix, so it
is out of scope by construction rather than by an exemption entry that could go
stale.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "nodedb" / "src"

# Frames built anywhere under here reach a native-protocol client.
SCANNED_PREFIX = "control/server/native/"

FORBIDDEN_RE = re.compile(r"\bNativeResponse\s*::\s*error\s*\(")


def mask_rust(source: str) -> str:
    """Mask comments and string/character literals while preserving newlines."""
    out = list(source)
    i = 0
    n = len(source)
    block_depth = 0
    while i < n:
        if block_depth:
            if source.startswith("/*", i):
                out[i : i + 2] = "  "
                block_depth += 1
                i += 2
            elif source.startswith("*/", i):
                out[i : i + 2] = "  "
                block_depth -= 1
                i += 2
            else:
                if source[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        if source.startswith("//", i):
            end = source.find("\n", i)
            end = n if end < 0 else end
            out[i:end] = " " * (end - i)
            i = end
            continue
        if source.startswith("/*", i):
            out[i : i + 2] = "  "
            block_depth = 1
            i += 2
            continue
        if source[i] in "\"'":
            quote = source[i]
            i += 1
            while i < n:
                if source[i] == "\\":
                    out[i] = " "
                    if i + 1 < n and source[i + 1] != "\n":
                        out[i + 1] = " "
                    i += 2
                    continue
                if source[i] == quote:
                    out[i] = " "
                    i += 1
                    break
                if source[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        i += 1
    return "".join(out)


def violations(source: str) -> list[int]:
    """Line numbers of unclassified frame constructions in `source`."""
    masked = mask_rust(source)
    return [
        masked.count("\n", 0, match.start()) + 1
        for match in FORBIDDEN_RE.finditer(masked)
    ]


def self_test() -> int:
    failed = False

    def check(name: str, condition: bool) -> None:
        nonlocal failed
        if not condition:
            print(f"self-test failed: {name}", file=sys.stderr)
            failed = True

    check("bare call is caught", violations("NativeResponse::error(seq, c, m)") == [1])
    check(
        "spaced call is caught",
        violations("NativeResponse :: error (seq, c, m)") == [1],
    )
    check(
        "error_with_code is allowed",
        violations("NativeResponse::error_with_code(seq, c, m, n)") == [],
    )
    check(
        "sqlstate_error is allowed",
        violations("sqlstate_error(seq, \"42P01\", msg)") == [],
    )
    check("comment is ignored", violations("// NativeResponse::error(a, b, c)") == [])
    check(
        "block comment is ignored",
        violations("/* NativeResponse::error(a, b, c) */") == [],
    )
    check(
        "string literal is ignored",
        violations('let s = "NativeResponse::error(a, b, c)";') == [],
    )
    check(
        "line number is reported",
        violations("fn f() {\n    NativeResponse::error(1, a, b)\n}") == [2],
    )
    if not failed:
        print("OK: native-error-frame gate self-tests passed.")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()

    scanned = SRC / SCANNED_PREFIX
    if not scanned.is_dir():
        print(
            f"ERROR: {SCANNED_PREFIX} does not exist — this gate is aimed at nothing.",
            file=sys.stderr,
        )
        return 1

    errors: list[str] = []
    for path in sorted(scanned.rglob("*.rs")):
        source = path.read_text(encoding="utf-8")
        for line in violations(source):
            errors.append(f"{path.relative_to(ROOT)}:{line}")

    if errors:
        print(
            "ERROR: native error frames must carry the numeric NodeDB code:",
            file=sys.stderr,
        )
        for error in errors:
            print(f"  {error}: builds a frame with `ndb_code == 0`", file=sys.stderr)
        print(
            "\nUse error_to_native / error_to_native_with_sqlstate when the site holds an\n"
            "Error, sqlstate_error when it only has a SQLSTATE, or\n"
            "NativeResponse::error_with_code when the code is already known.",
            file=sys.stderr,
        )
        return 1

    print("OK: native error frames carry their numeric code.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
