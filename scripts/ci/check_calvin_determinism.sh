#!/usr/bin/env bash
# Calvin determinism gate.
#
# Every file in the Calvin write path must not use non-deterministic APIs
# without an explicit opt-out marker.  Any unmarked occurrence fails the gate.
#
# Marker form: `// no-determinism: <reason>` placed on the same line as the
# offending call OR on the directly-preceding source line.  This mirrors the
# `// no-governor:` and `// no-objectstore:` conventions elsewhere in this
# repo.
#
# Structural limitation: this grep gate only catches DIRECT calls to the
# forbidden symbols.  Banned APIs hidden behind wrapper functions are NOT
# caught here.  Known wrappers tracked separately:
#   - `OrdinalClock::next_ordinal()` wraps HLC internals — inspected in the
#     engine audit and the property test (`tests/calvin_determinism_contract.rs`).
#   - `wall_now_ms()` in `nodedb/src/engine/document/store/engine/batch.rs`
#     wraps `SystemTime::now()` — real violation, engine fix required.
#   - `current_ms()` in `nodedb/src/engine/kv/mod.rs` wraps
#     `SystemTime::now()` — real violation, engine fix required.
#   - `wall_now_ms()` / `current_ms()` call sites in handlers are caught only
#     if the wrapper itself is in the scan list.
#
# Excluded from every scan:
#   - Lines whose first non-whitespace characters are `//` (pure comment).
#   - Lines whose first non-whitespace characters are `*`  (doc-comment cont).
#   - Bodies of inline `#[cfg(test)] mod tests { ... }` blocks.
#   - Rust test-only source files named `tests.rs`.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

scan_paths=(
    "nodedb/src/data/executor/handlers/control/calvin.rs"
    "nodedb/src/data/executor/handlers/transaction"
    "nodedb/src/data/executor/handlers/graph.rs"
    "nodedb/src/data/executor/handlers/timeseries/ingest.rs"
    "nodedb/src/engine/document/store/engine"
    "nodedb/src/engine/kv/mod.rs"
    "nodedb-vector/src/codec_index"
    "nodedb-columnar/src/writer/encode.rs"
    "nodedb-cluster/src/calvin"
    "nodedb/src/control/cluster/calvin"
)

# Patterns that indicate non-determinism.
patterns=(
    'HashMap\.(iter|values|keys|drain|into_iter)\b'
    'HashSet\.(iter|drain|into_iter)\b'
    'SystemTime::now\b'
    'Instant::now\b'
    'thread_rng\(\)'
    'rand::random\b'
    'Uuid::new_v4\b'
    'DefaultHasher::new\b'
    'RandomState::new\b'
)

violations=()

for rel in "${scan_paths[@]}"; do
    target="$ROOT/$rel"
    [ -e "$target" ] || continue

    combined_pattern=$(
        IFS='|'
        echo "${patterns[*]}"
    )

    while IFS= read -r match; do
        file="${match%%:*}"
        rest="${match#*:}"
        lineno="${rest%%:*}"

        # Separate Rust test modules are test-only, like inline cfg(test) bodies.
        [[ "$file" == */tests.rs ]] && continue

        # Skip pure comment lines (first non-whitespace is //).
        line_content=$(sed -n "${lineno}p" "$file")
        stripped="${line_content#"${line_content%%[![:space:]]*}"}"
        case "$stripped" in
            //*) continue ;;
            \**) continue ;;
        esac

        # Skip if inside a #[cfg(test)] mod tests { ... } block.
        last_cfg=$(awk -v n="$lineno" 'NR<=n && /^#\[cfg\(test\)\]/ {x=NR} END{print x+0}' "$file")
        if [ "$last_cfg" -gt 0 ]; then
            close_after=$(awk -v a="$last_cfg" -v b="$lineno" 'NR>a && NR<b && /^}/ {x=NR} END{print x+0}' "$file")
            [ "$close_after" -eq 0 ] && continue
        fi

        # Check for marker on same line or directly-preceding line.
        prev=$((lineno > 1 ? lineno - 1 : 1))
        window=$(sed -n "${prev},${lineno}p" "$file")
        echo "$window" | grep -q 'no-determinism:' && continue

        violations+=("${file#"$ROOT/"}:$lineno")
    done < <(grep -rEHn "($combined_pattern)" "$target" 2>/dev/null \
             | grep -v ':[[:space:]]*//' \
             | grep -v ':[[:space:]]*\*' \
             || true)
done

if [ ${#violations[@]} -gt 0 ]; then
    echo "FAIL: ${#violations[@]} non-deterministic site(s) in Calvin write path:"
    printf '  %s\n' "${violations[@]}"
    echo
    echo "Each site must either:"
    echo "  - be fixed to use a deterministic alternative, or"
    echo "  - carry a '// no-determinism: <reason>' marker on the same line"
    echo "    or the directly-preceding line."
    echo
    echo "Forbidden patterns:"
    printf '  %s\n' "${patterns[@]}"
    exit 1
fi
echo "OK: Calvin determinism gate clean."
