#!/bin/bash
# Cold/hot ClickBench sweep over an already-loaded infino table.
#
# For each query: drop the OS page cache once, then run TRIES times.
#   t1  = cold  (cold page cache; embedded engine also starts cold)
#   t2/t3 = hot (page cache warmed by t1)
#   hot = min(t2, t3)   — the ClickBench hot metric
#
# Emits a cold/hot table to stdout and per-try rows to result.csv
# (query_num,try,seconds). Run from the infino/ dir after ./load.
# Needs sudo for drop_caches (Linux).
set -e

QUERIES="${BENCH_QUERIES_FILE:-queries.sql}"
TRIES="${BENCH_TRIES:-3}"

# Attach a persistent disk cache so queries range-read only the projected
# columns (t1 fills it, t2/t3 hit the mmap) instead of the tier-3
# whole-superfile read. Exported so the per-query ./query child inherits it.
# Set BENCH_NO_CACHE=1 to leave it unset and measure the no-cache baseline.
if [ -z "${BENCH_NO_CACHE:-}" ]; then
    export INFINO_CACHE_DIR="${INFINO_CACHE_DIR:-./cache}"
fi

: > result.csv
printf "%-4s %12s %12s %12s %12s\n" Q cold t2 t3 hot

n=1
while IFS= read -r q; do
    [ -z "$q" ] && continue
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    ts=()
    for i in $(seq 1 "$TRIES"); do
        printf '%s\n' "$q" | ./query >/dev/null 2>/tmp/qerr.$$ || true
        sec=$(tail -1 /tmp/qerr.$$)
        ts+=("$sec")
        echo "${n},${i},${sec}" >> result.csv
    done
    hot=$(awk -v a="${ts[1]}" -v b="${ts[2]}" 'BEGIN{print (a<b)?a:b}')
    printf "%-4s %12s %12s %12s %12s\n" "$n" "${ts[0]}" "${ts[1]}" "${ts[2]}" "$hot"
    n=$((n + 1))
done < "$QUERIES"
rm -f /tmp/qerr.$$

# Hot geometric mean: per query, hot = min over tries >= 2.
awk -F, '$2>1 { if (!($1 in m) || $3 < m[$1]) m[$1] = $3 }
         END { for (k in m) { s += log(m[k]); c++ } printf "\nhot geomean: %.4fs over %d queries\n", exp(s/c), c }' result.csv
