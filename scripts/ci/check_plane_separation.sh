#!/usr/bin/env bash
# Plane-separation and layering gate.
#
# NodeDB's three-plane execution model (Control / Data / Event) is a
# correctness boundary, not a performance hint. This gate enforces four
# structural invariants:
#
#   1. Data Plane purity: `nodedb/src/data/**` must not import or call
#      `tokio::*`. The Data Plane is single-threaded thread-per-core
#      (TPC) and `!Send` by construction. Any tokio dependency leaks
#      cross-thread scheduling non-determinism back into the Data Plane.
#
#   2. Control Plane io_uring purity: `nodedb/src/control/**` must not
#      use `io_uring` / `tokio_uring` directly. The Control Plane runs
#      on the standard tokio thread pool; storage I/O happens in the
#      Data Plane via the SPSC bridge.
#
#   3. Bridge boundary purity: `nodedb-bridge/src/**` and
#      `nodedb/src/bridge/**` must not contain `Arc<Mutex<…>>` or
#      `Arc<RwLock<…>>`. The bridge is a lock-free SPSC ring; lock-based
#      sync at the boundary is by definition the wrong shape.
#
#   4. Security-layer direction: `nodedb/src/control/security/**` must not
#      reference `crate::control::server::*`. `security` is the lower
#      layer — the catalog, identities, scopes and policies that `server`
#      is built on top of. An item both need belongs in `security` (or a
#      module below both), with `server` importing it from there; a
#      `security` -> `server` edge makes the dependency a cycle in intent
#      and forces the lower layer to be rebuilt around the higher one.
#      Doc-comment cross-references are not code edges and are excluded
#      by the pure-comment skip below.
#
# Marker form: `// no-plane-separation: <reason>` placed on the same
# line as the offending construct OR on the directly-preceding source
# line. Mirrors `// no-determinism:`, `// no-governor:`, and
# `// no-objectstore:` conventions.
#
# Excluded from every scan:
#   - Lines whose first non-whitespace characters are `//` (pure comment).
#   - Lines whose first non-whitespace characters are `*`  (doc-comment cont).
#   - Bodies of inline `#[cfg(test)] mod tests { ... }` blocks.
#   - String / metric-name occurrences (e.g. `"io_uring_submissions"`)
#     are inert data, not behavior — only `use` / path-call forms fail.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Each rule is (label, scan_path, regex). Four rules total.
rule_labels=(
    "Data Plane: no tokio in nodedb/src/data/"
    "Control Plane: no io_uring in nodedb/src/control/"
    "Bridge boundary: no Arc<Mutex>/Arc<RwLock>"
    "Layering: no crate::control::server in nodedb/src/control/security/"
)
rule_paths=(
    "nodedb/src/data"
    "nodedb/src/control"
    "nodedb-bridge/src nodedb/src/bridge"
    "nodedb/src/control/security"
)
# Patterns:
#   - Rule 1: `use tokio` (imports), `tokio::` (path calls). Inert string
#     refs and the literal `tokio_uring` (rule 2's domain) are excluded.
#   - Rule 2: `use io_uring`, `use tokio_uring`, and `tokio_uring::` /
#     `io_uring::` path calls.
#   - Rule 3: `Arc<Mutex<` / `Arc<RwLock<` and tokio-async variants
#     `Arc<tokio::sync::Mutex<` / `Arc<tokio::sync::RwLock<`.
#   - Rule 4: `use crate::control::server` imports and inline
#     `crate::control::server::` path references.
rule_patterns=(
    '^[[:space:]]*use[[:space:]]+tokio\b|\btokio::'
    '^[[:space:]]*use[[:space:]]+(io_uring|tokio_uring)\b|\b(io_uring|tokio_uring)::'
    'Arc<[[:space:]]*Mutex<|Arc<[[:space:]]*RwLock<|Arc<[[:space:]]*tokio::sync::(Mutex|RwLock)<'
    '^[[:space:]]*use[[:space:]]+crate::control::server\b|\bcrate::control::server::'
)

violations=()
checked=0

for i in "${!rule_labels[@]}"; do
    label="${rule_labels[$i]}"
    paths="${rule_paths[$i]}"
    pattern="${rule_patterns[$i]}"

    for rel in $paths; do
        target="$ROOT/$rel"
        [ -e "$target" ] || continue

        while IFS= read -r match; do
            file="${match%%:*}"
            rest="${match#*:}"
            lineno="${rest%%:*}"
            checked=$((checked + 1))

            # Skip pure comment lines (first non-whitespace is // or *).
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

            # Check for opt-out marker on same line or directly-preceding line.
            prev=$((lineno > 1 ? lineno - 1 : 1))
            window=$(sed -n "${prev},${lineno}p" "$file")
            echo "$window" | grep -q 'no-plane-separation:' && continue

            violations+=("[$label] ${file#"$ROOT/"}:$lineno")
        done < <(grep -rEHn "$pattern" "$target" 2>/dev/null \
                 | grep -v ':[[:space:]]*//' \
                 | grep -v ':[[:space:]]*\*' \
                 || true)
    done
done

if [ ${#violations[@]} -gt 0 ]; then
    echo "FAIL: ${#violations[@]} plane-separation violation(s):"
    printf '  %s\n' "${violations[@]}"
    echo
    echo "Each site must either:"
    echo "  - be moved to the correct plane, or"
    echo "  - carry a '// no-plane-separation: <reason>' marker on the"
    echo "    same line or the directly-preceding line."
    echo
    echo "Plane rules:"
    echo "  - Data Plane (nodedb/src/data/) is !Send + TPC: no tokio."
    echo "  - Control Plane (nodedb/src/control/) is Send + Sync: no io_uring."
    echo "  - Bridge (nodedb-bridge/, nodedb/src/bridge/) is lock-free SPSC:"
    echo "    no Arc<Mutex<...>> / Arc<RwLock<...>>."
    echo "  - security (nodedb/src/control/security/) is below server:"
    echo "    move the shared item down into security and import it there"
    echo "    from server; never re-export a server item from security."
    exit 1
fi
echo "OK: plane-separation and layering gate clean."
