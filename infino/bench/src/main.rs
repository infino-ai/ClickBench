//! ClickBench harness for infino (Rust binding).
//!
//! Subcommands:
//!   load   — ingest parquet (glob INFINO_SRC) into a persisted infino table.
//!   query  — read one SQL statement from stdin, time query_sql, print row count
//!            to stdout and elapsed seconds to stderr (the ClickBench contract).
//!   check  — verify the connection opens.
//!
//! Env: INFINO_URI (default ./data), INFINO_SRC (default hits.parquet),
//!      INFINO_MAX_ROWS (0 = all), INFINO_STORAGE_* (storage_options),
//!      INFINO_CACHE_DIR.

use std::env;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::compute::cast;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use infino::{
    connect, connect_with, CompactionSettings, ConnectOptions, IndexSpec, OptimizeOptions,
};

type R<T> = Result<T, Box<dyn Error>>;

const BATCH_ROWS: usize = 1_000_000;

fn uri() -> String {
    env::var("INFINO_URI").unwrap_or_else(|_| "./data".to_string())
}

fn open() -> R<infino::Connection> {
    let mut opts = ConnectOptions::new();
    let mut custom = false;
    for (k, v) in env::vars() {
        if let Some(key) = k.strip_prefix("INFINO_STORAGE_") {
            opts = opts.with_storage_option(key.to_lowercase(), v);
            custom = true;
        }
    }
    if let Ok(dir) = env::var("INFINO_CACHE_DIR") {
        opts = opts.with_cache_dir(dir);
        custom = true;
    }

    // Raise the disk-cache budget above the 10 GiB default so a large corpus
    // (e.g. 100M rows, tens of GB of superfiles) fits on a big disk instead of
    // thrashing / falling back to range-only reads. Bytes.
    if let Some(b) = env::var("INFINO_CACHE_BUDGET")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        opts = opts.with_cache_budget_bytes(b);
        custom = true;
    }

    Ok(if custom {
        connect_with(uri(), opts)?
    } else {
        connect(uri())?
    })
}

/// Target arrow type for a source parquet field. infino queries its own table
/// (not an external parquet view), so we apply the same adjustments the
/// datafusion variant does inline at query time:
///   EventDate: integer day count -> DATE.
///   text (Binary): -> Utf8 so LIKE / REGEXP_REPLACE / length work.
fn target_type(f: &Field) -> DataType {
    if f.name() == "EventDate" {
        return DataType::Date32;
    }
    match f.data_type() {
        DataType::Binary | DataType::LargeBinary => DataType::Utf8,
        other => other.clone(),
    }
}

fn cast_batch(batch: &RecordBatch, target: &SchemaRef) -> R<RecordBatch> {
    let mut cols = Vec::with_capacity(target.fields().len());
    for (i, f) in target.fields().iter().enumerate() {
        let col = batch.column(i);
        let out = if f.name() == "EventDate" {
            // int -> int32 -> Date32, matching datafusion's CAST(CAST(.. AS INTEGER) AS DATE).
            cast(&cast(col, &DataType::Int32)?, &DataType::Date32)?
        } else if col.data_type() != f.data_type() {
            cast(col, f.data_type())?
        } else {
            col.clone()
        };
        cols.push(out);
    }
    Ok(RecordBatch::try_new(target.clone(), cols)?)
}

fn load() -> R<()> {
    let src = env::var("INFINO_SRC").unwrap_or_else(|_| "hits.parquet".to_string());
    let max_rows: Option<usize> = env::var("INFINO_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0);

    let mut files: Vec<PathBuf> = glob::glob(&src)?.filter_map(Result::ok).collect();
    files.sort();
    if files.is_empty() {
        return Err(format!("no parquet files match {src:?}").into());
    }

    // Target schema from the first file's schema.
    let src_schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&files[0])?)?
        .schema()
        .clone();
    let fields: Vec<Field> = src_schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), target_type(f), f.is_nullable()))
        .collect();
    let target: SchemaRef = Arc::new(Schema::new(fields));

    let db = open()?;
    if db.list_tables()?.iter().any(|t| t == "hits") {
        db.drop_table("hits", true)?;
    }
    let table = db.create_table("hits", target.clone(), IndexSpec::new())?;

    let mut appended: usize = 0;
    'files: for path in &files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
            .with_batch_size(BATCH_ROWS)
            .build()?;
        for batch in reader {
            let mut batch = batch?;
            if let Some(max) = max_rows {
                if appended + batch.num_rows() > max {
                    batch = batch.slice(0, max - appended);
                }
            }
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            table.append(&cast_batch(&batch, &target)?)?;
            appended += n;
            if max_rows.is_some_and(|max| appended >= max) {
                break 'files;
            }
        }
    }

    // Compact per-batch superfiles into fewer, uniform segments. Part of the
    // honest load cost.
    table.optimize(&optimize_options())?;
    println!("ingested {appended} rows");
    Ok(())
}

/// INFINO_TARGET_SF_MB sizes the compacted superfiles. Unset = infino's own
/// default (~1 GiB target). Set it to size segments to the machine — e.g. 256
/// on an 8-core box yields several balanced segments for parallel scan instead
/// of one large file plus small leftovers. min_fill_percent is dropped to 1 so
/// a one-shot optimize actually merges the small tail rather than leaving it.
fn optimize_options() -> OptimizeOptions {
    match env::var("INFINO_TARGET_SF_MB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(mb) => OptimizeOptions::compact(CompactionSettings {
            target_superfile_size_mb: mb,
            min_fill_percent: 1,
            max_memory_mb: mb + 2048,
        }),
        None => OptimizeOptions::default(),
    }
}

fn query() -> R<()> {
    let mut sql = String::new();
    std::io::stdin().read_to_string(&mut sql)?;
    // Open once, then run the query INFINO_QUERY_TRIES times in this one
    // process: try 1 is cold (page cache dropped, connection just opened),
    // tries 2/3 reuse the warm connection — matching how ClickBench keeps a
    // warm server across a query's tries. One elapsed line per try on stderr;
    // row count on stdout. (Own env var, not BENCH_TRIES, so the shared
    // ClickBench driver — which loops ./query itself — still sees one try.)
    let tries: usize = env::var("INFINO_QUERY_TRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1);

    let db = open()?;

    let mut rows = 0;

    for _ in 0..tries {
        let start = Instant::now();
        let batches = db.query_sql(&sql)?;
        rows = batches.iter().map(|b| b.num_rows()).sum();
        eprintln!("{:.6}", start.elapsed().as_secs_f64());
    }

    println!("{rows} rows");

    Ok(())
}

fn check() -> R<()> {
    open()?;
    println!("ok");
    Ok(())
}

fn main() {
    let cmd = env::args().nth(1).unwrap_or_default();
    let result = match cmd.as_str() {
        "load" => load(),
        "query" => query(),
        "check" => check(),
        other => Err(format!("unknown subcommand {other:?} (want load|query|check)").into()),
    };
    if let Err(e) = result {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
