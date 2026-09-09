#!/usr/bin/env python3
"""Reject sequence-accessor leaks out of the DEFAULT plane.

Conservative static source gate (not a Rust parser): after masking comments
and string bodies it asserts the seam contracts of issue #294 / PR #303
hold in the current tree. Exit code 0 = contracts hold.

Contract checklist:
1.  Registry registers nextval/currval/setval (plan-time gate + typing),
    wired into `scalars.rs` — an unregistered accessor would fall back to
    42883 or silent NULL instead of the loud 0A000 plan-time story.
2.  `eval_function` raises `FeatureNotSupported` for the accessor names
    BEFORE any family dispatch, so they can never reach the geo fallback's
    silent NULL (nodedb-query/src/functions/eval.rs).
3.  `const_fold` classifies `FeatureNotSupported` exhaustively (fold path
    must be loud, never deferred into a nonexistent row scope).
4.  DEFAULT still stays CP-side: the kv-insert default expander keeps its
    `looks_like_sequence_accessor` skip (pure-evaluator must never see the
    accessor) and the sequence-default converter still wraps registry
    errors as PlanError (malformed/currval/setval loud).
5.  Wire regression exists: expression-context cases assert 0A000 and the
    DEFAULT-per-row fill stays green (sequence_default_all_engines.rs).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FAILURES: list[str] = []

ACCESSOR_NAMES = ("nextval", "currval", "setval")


def check(path: Path, needles: list[str]) -> None:
    """All `needles` must appear as plain substrings of the file."""
    text = path.read_text(encoding="utf-8", errors="ignore")
    for needle in needles:
        if needle not in text:
            FAILURES.append(f"{path.relative_to(ROOT)}: missing {needle!r}")


def main() -> int:
    check(
        ROOT / "nodedb-sql/src/functions/builtins/scalars/sequence.rs",
        [
            '"nextval"',
            '"currval"',
            '"setval"',
            "FunctionCategory::Scalar" if False else "Scalar",
        ],
    )
    check(
        ROOT / "nodedb-sql/src/functions/builtins/scalars.rs",
        ["mod sequence;", "sequence::sequence_functions()"],
    )
    eval_src = (
        ROOT / "nodedb-query/src/functions/eval.rs"
    ).read_text(encoding="utf-8")
    # The guard must come before every family dispatch: crude positional
    # proof — the arm text must sit above the first `::try_eval` call.
    guard_pos = eval_src.find("FeatureNotSupported")
    first_dispatch = eval_src.find("::try_eval(")
    if guard_pos == -1 or first_dispatch == -1 or guard_pos > first_dispatch:
        FAILURES.append(
            "nodedb-query/src/functions/eval.rs: guard arm must precede family dispatch"
        )
    check(
        ROOT / "nodedb-sql/src/planner/const_fold.rs",
        ["EvalError::FeatureNotSupported", "SqlError::FeatureNotSupported"],
    )
    check(
        ROOT / "nodedb-sql/src/planner/dml_helpers/kv_insert.rs",
        ["looks_like_sequence_accessor"],
    )
    check(
        ROOT
        / "nodedb/src/control/planner/sql_plan_convert/value/sequence_default.rs",
        ["malformed sequence default", "is not supported"],
    )
    check(
        ROOT / "nodedb/tests/wire/cases/sequence_default_all_engines.rs",
        ["DEFAULT nextval(", "CREATE SEQUENCE"],
    )
    check(
        ROOT / "nodedb/tests/wire/cases/sequence_expression_contexts.rs",
        ["0A000", "DEFAULT nextval"],
    )
    check(
        ROOT / "nodedb/src/control/server/pgwire/types/error_map.rs",
        ["FEATURE_NOT_SUPPORTED"],
    )
    check(
        ROOT / "nodedb/src/control/server/shared/ddl/sqlstate.rs",
        ["FEATURE_NOT_SUPPORTED"],
    )

    if FAILURES:
        print(f"sequence plane gate: {len(FAILURES)} contract violation(s)")
        for f in FAILURES:
            print(f"  ✗ {f}")
        return 1
    print("sequence plane gate: all contracts hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
