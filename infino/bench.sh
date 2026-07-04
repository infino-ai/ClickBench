#!/bin/bash
# Cold/hot ClickBench sweep over an already-loaded infino table.
#
# For each query: drop the OS page cache once, then run the query TRIES times
# **in a single process** (the connection is opened once):
#   t1    = cold  (cold page cache; the connection just opened)
#   t2/t3 = hot   (page cache + the warm connection carried over from t1)
#   hot   = min(t2, t3)   — the ClickBench hot metric
# Running the tries in one process mirrors how ClickBench keeps a warm server
# across a query's tries (the spec restarts only before the cold run), rather
# than restarting the embedded engine between tries.
#
# Emits a cold/hot table to stdout and per-try rows to result.csv
# (query_num,try,seconds). Run from the infino/ dir after ./load.
# Needs sudo for drop_caches (Linux).
set -e

QUERIES="${BENCH_QUERIES_FILE:-queries.sql}"
TRIES="${BENCH_TRIES:-3}"
# The query binary runs the SQL this many times in one process (see above).
export INFINO_QUERY_TRIES="$TRIES"

# Attach a persistent disk cache so queries range-read only the projected
# columns instead of the tier-3 whole-superfile read. Exported so the
# ./query child inherits it. Set BENCH_NO_CACHE=1 for the no-cache baseline.
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
    # One process; ./query emits TRIES elapsed lines on stderr (one per try).
    printf '%s\n' "$q" | ./query >/dev/null 2>/tmp/qerr.$$ || true
    mapfile -t ts < /tmp/qerr.$$
    for i in $(seq 1 "$TRIES"); do
        echo "${n},${i},${ts[$((i - 1))]}" >> result.csv
    done
    hot=$(awk -v a="${ts[1]}" -v b="${ts[2]}" 'BEGIN{print (a<b)?a:b}')
    printf "%-4s %12s %12s %12s %12s\n" "$n" "${ts[0]}" "${ts[1]}" "${ts[2]}" "$hot"
    n=$((n + 1))
done < "$QUERIES"
rm -f /tmp/qerr.$$

# Hot geometric mean: per query, hot = min over tries >= 2.
awk -F, '$2>1 { if (!($1 in m) || $3 < m[$1]) m[$1] = $3 }
         END { for (k in m) { s += log(m[k]); c++ } printf "\nhot geomean: %.4fs over %d queries\n", exp(s/c), c }' result.csv
