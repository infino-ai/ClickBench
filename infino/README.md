# infino

[infino](https://github.com/infino-ai/infino) is an embedded retrieval engine
that runs SQL, full-text (BM25), and vector search over Parquet on local disk or
object storage. ClickBench exercises only the SQL path.

The harness is a small Rust binary (`bench/`) that depends on the published
`infino` crate — no Python, no server. It ingests the parquet into a persisted
infino table and runs each query via `Connection::query_sql`.

## Build

```sh
./install        # installs rustup if absent, then cargo build --release
```

## Run (automated ClickBench flow)

```sh
./benchmark.sh   # download -> load -> per-query 3-try sweep (via lib/benchmark-common.sh)
```

## Run manually (e.g. a 10M subset)

```sh
# Fetch N partitions instead of the full single file:
seq 0 10 | xargs -P11 -I{} wget -q --continue \
  https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet

INFINO_SRC="hits_*.parquet" INFINO_MAX_ROWS=10000000 ./load   # ingest once
./bench.sh                                                    # cold/hot sweep
```

`./bench.sh` drops the page cache before each query's tries, so `t1` is cold and
`t2/t3` are hot (`hot = min(t2,t3)`); it writes `result.csv` and prints a
cold/hot table plus the hot geomean.

## Environment

| var | default | meaning |
|---|---|---|
| `INFINO_URI` | `./data` | backend: local path, `az://…`, `s3://…` |
| `INFINO_SRC` | `hits.parquet` | glob for source parquet |
| `INFINO_MAX_ROWS` | all | cap total ingested rows (e.g. `10000000`) |
| `INFINO_TARGET_SF_MB` | infino default (~1 GiB) | compacted superfile target size; set to size segments to the machine |
| `INFINO_STORAGE_*` | — | passed as `storage_options` (e.g. `INFINO_STORAGE_AZURE_STORAGE_ACCOUNT_NAME`) |
| `INFINO_CACHE_DIR` | — | local cache dir for object-storage backends |

## Type handling

infino queries its own table rather than an external parquet view, so the two
adjustments the `datafusion` variant does inline at query time are done here at
ingest (`bench/src/main.rs`):

- `EventDate` (integer day count) → `DATE`, so date-range predicates work
  without per-query casts.
- Text columns (Parquet `BYTE_ARRAY`/binary) → UTF-8, so `LIKE`,
  `REGEXP_REPLACE`, and `length` operate on strings.

`EventTime` keeps its native integer type; the queries wrap it in
`to_timestamp_seconds(...)`, so `queries.sql` is identical to the `datafusion`
variant's. Every table carries an auto-generated `_id` column, so query 24
(`SELECT *`) returns it as an extra trailing column.
