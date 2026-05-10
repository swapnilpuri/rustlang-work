//! High-throughput Oracle -> Kinetica ETL, built on `oracle-rs`
//! (pure Rust async TNS driver -- no Oracle Instant Client required).
//!
//! Pattern (chosen for ~150M-row workloads):
//!
//!   [reader-0 task]\
//!   [reader-1 task]----async-channel (bounded)----[writer-0 task]
//!        ...           Vec<Value> batches          [writer-1 task]
//!   [reader-N task]/                                     ...
//!
//! Why this shape:
//!   * `oracle-rs` is async/Tokio-native. Each reader is a `tokio::spawn`
//!     with its OWN `Connection` -- the docs explicitly note that operations
//!     on a Connection are serialized via an internal mutex, so parallelism
//!     requires multiple Connections.
//!   * Reads are partitioned with ORA_HASH so the N readers fetch disjoint
//!     row sets in parallel without coordination.
//!   * **Streaming**: query() returns the first chunk plus a cursor_id; we
//!     then call fetch_more() in a loop until has_more_rows is false. This
//!     keeps memory bounded -- we never materialize 18M+ rows at once.
//!   * `async-channel` is bounded MPMC: readers block on send when writers
//!     fall behind (= backpressure, prevents OOM at 150M rows).
//!   * Records are accumulated into batches of `--batch-size` and POSTed to
//!     Kinetica's `/insert/records/json`; per-row inserts would be 1000x slower.
//!   * One async `reqwest::Client`, shared by all writers, pools TCP+TLS
//!     connections so handshakes are amortized.
//!
//! Build:   cargo build --release
//! Run:     ./target/release/ora2kinetica --threads 8 --writers 4 \
//!              --oracle-user scott --oracle-password tiger \
//!              --oracle-dsn dbhost:1521/ORCLPDB1 \
//!              --sql 'SELECT id, name, amount, created_at FROM big_table WHERE {partition_clause}' \
//!              --kinetica-url https://kinetica.host:9191 --kinetica-user admin \
//!              --kinetica-password ***** --kinetica-table big_table

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use oracle_rs::{ColumnInfo, Connection, Row, Value as OraValue};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Map, Value as JsonValue};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// CLI -- intentionally tiny: two file paths and nothing else.
// All operational parameters live in the config file so invocations are
// reproducible, secrets aren't visible in `ps`, and tuning is version-controllable.
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(version, about = "Parallel Oracle -> Kinetica bulk loader (oracle-rs)")]
struct Cli {
    /// Path to the TOML config file (Oracle creds, Kinetica target, tuning).
    /// See config.example.toml for the full schema.
    #[arg(short = 'c', long)]
    config: PathBuf,

    /// Path to the file containing the SQL template. The contents must include
    /// the literal token `{partition_clause}`, which the loader replaces per
    /// reader task with `ORA_HASH(<partition_column>, N-1) = TID`.
    #[arg(short = 's', long)]
    sql_file: PathBuf,
}

// ---------------------------------------------------------------------------
// Config -- deserialized from TOML. Sectioned to mirror the file layout:
//   [oracle]    connection + partitioning + fetch tuning
//   [kinetica]  REST endpoint + auth + target table
//   [pipeline]  thread/writer counts, channel size, batch size, retries
// `serde(default)` on individual fields lets us ship sensible defaults without
// requiring users to spell every knob out.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Config {
    oracle:   OracleConfig,
    kinetica: KineticaConfig,
    #[serde(default)]
    pipeline: PipelineConfig,
}

#[derive(Debug, Deserialize)]
struct OracleConfig {
    /// EZConnect string, e.g. "host:1521/SERVICE"
    dsn:      String,
    user:     String,
    password: String,

    /// Column to hash for partitioning. ROWID works for any table without a
    /// suitable key. For an index-friendly partition, set a numeric PK column.
    #[serde(default = "default_partition_column")]
    partition_column: String,

    /// Rows per fetch_more round-trip. 5k-20k typical.
    #[serde(default = "default_fetch_size")]
    fetch_size: u32,
}

#[derive(Debug, Deserialize)]
struct KineticaConfig {
    url:      String,
    user:     String,
    password: String,
    table:    String,
}

#[derive(Debug, Deserialize)]
struct PipelineConfig {
    /// Number of concurrent Oracle reader tasks (= concurrent Connections).
    #[serde(default = "default_threads")]
    threads: usize,

    /// Number of concurrent Kinetica writer tasks.
    #[serde(default = "default_writers")]
    writers: usize,

    /// Records per HTTP POST to Kinetica.
    #[serde(default = "default_batch_size")]
    batch_size: usize,

    /// Max queued batches between readers and writers.
    #[serde(default = "default_channel_capacity")]
    channel_capacity: usize,

    /// Retries per failed Kinetica POST (exponential backoff).
    #[serde(default = "default_max_retries")]
    max_retries: u32,
}

// `Default` so [pipeline] can be omitted entirely from the TOML file.
impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            threads:          default_threads(),
            writers:          default_writers(),
            batch_size:       default_batch_size(),
            channel_capacity: default_channel_capacity(),
            max_retries:      default_max_retries(),
        }
    }
}

// serde(default = "...") needs free functions, not literals.
fn default_partition_column() -> String { "ROWID".to_string() }
fn default_fetch_size()       -> u32    { 10_000 }
fn default_threads()          -> usize  { 8 }
fn default_writers()          -> usize  { 4 }
fn default_batch_size()       -> usize  { 5_000 }
fn default_channel_capacity() -> usize  { 64 }
fn default_max_retries()      -> u32    { 5 }

impl Config {
    fn load(path: &std::path::Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config from {}", path.display()))?;
        let cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("failed to parse TOML config at {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.pipeline.threads == 0 {
            return Err(anyhow!("pipeline.threads must be >= 1"));
        }
        if self.pipeline.writers == 0 {
            return Err(anyhow!("pipeline.writers must be >= 1"));
        }
        if self.pipeline.batch_size == 0 {
            return Err(anyhow!("pipeline.batch_size must be >= 1"));
        }
        if self.pipeline.channel_capacity == 0 {
            return Err(anyhow!("pipeline.channel_capacity must be >= 1"));
        }
        Ok(())
    }
}

type Batch = Vec<JsonValue>;

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let cfg = Arc::new(Config::load(&cli.config)?);

    // Read the SQL file once at startup, validate placeholder, share via Arc.
    let sql_template = std::fs::read_to_string(&cli.sql_file)
        .with_context(|| format!("failed to read SQL from {}", cli.sql_file.display()))?;

    if !sql_template.contains("{partition_clause}") {
        return Err(anyhow!(
            "SQL file must contain the literal placeholder `{{partition_clause}}` \
             so each reader can be given its own slice of the table"
        ));
    }
    let sql_template = Arc::new(sql_template);

    log::info!(
        "starting: {} reader tasks, {} writer tasks, batch={}, channel_cap={}, fetch_size={}",
        cfg.pipeline.threads, cfg.pipeline.writers, cfg.pipeline.batch_size,
        cfg.pipeline.channel_capacity, cfg.oracle.fetch_size
    );

    let started     = Instant::now();
    let read_count  = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));
    let stop_flag   = Arc::new(AtomicBool::new(false));

    // Bounded MPMC async channel = backpressure. Readers .send().await blocks
    // when writers lag, so RAM is capped regardless of total row count.
    let (tx, rx) = async_channel::bounded::<Batch>(cfg.pipeline.channel_capacity);

    // One reqwest::Client shared by all writers -- it pools TCP+TLS
    // connections internally and is cheap to clone.
    let http = Client::builder()
        .pool_max_idle_per_host(cfg.pipeline.writers * 2)
        .tcp_keepalive(Duration::from_secs(30))
        .timeout(Duration::from_secs(180))
        .build()
        .context("build reqwest client")?;

    // ---- spawn writers first so the channel has consumers
    let mut writer_handles = Vec::with_capacity(cfg.pipeline.writers);
    for w in 0..cfg.pipeline.writers {
        let rx   = rx.clone();
        let cfg  = Arc::clone(&cfg);
        let http = http.clone();
        let cnt  = Arc::clone(&write_count);
        let stop = Arc::clone(&stop_flag);
        writer_handles.push(tokio::spawn(async move {
            writer_task(w, rx, cfg, http, cnt, stop).await
        }));
    }
    drop(rx); // only writer tasks should hold receivers from here on

    // ---- spawn readers
    let mut reader_handles = Vec::with_capacity(cfg.pipeline.threads);
    for t in 0..cfg.pipeline.threads {
        let tx  = tx.clone();
        let cfg = Arc::clone(&cfg);
        let sql = Arc::clone(&sql_template);
        let cnt = Arc::clone(&read_count);
        reader_handles.push(tokio::spawn(async move {
            reader_task(t, cfg, sql, tx, cnt).await
        }));
    }
    // Closing our copy of tx is essential: when the LAST sender (held by the
    // readers) is dropped, recv() in writers returns Err and they exit cleanly.
    drop(tx);

    // ---- progress reporter
    let progress_handle = {
        let read  = Arc::clone(&read_count);
        let write = Arc::clone(&write_count);
        tokio::spawn(async move {
            let t0 = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let r = read.load(Ordering::Relaxed);
                let w = write.load(Ordering::Relaxed);
                let elapsed = t0.elapsed().as_secs_f64().max(0.001);
                log::info!(
                    "progress: read={r} written={w} in_flight={} ({:.0} rows/s)",
                    r.saturating_sub(w),
                    w as f64 / elapsed
                );
            }
        })
    };

    // ---- await readers, then writers
    let mut first_err: Option<anyhow::Error> = None;

    for h in reader_handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                log::error!("reader failed: {e:#}");
                first_err.get_or_insert(e);
                stop_flag.store(true, Ordering::Relaxed);
            }
            Err(join_err) => {
                log::error!("reader join error: {join_err}");
                stop_flag.store(true, Ordering::Relaxed);
            }
        }
    }
    log::info!("all readers complete: {} rows", read_count.load(Ordering::Relaxed));

    for h in writer_handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                log::error!("writer failed: {e:#}");
                first_err.get_or_insert(e);
            }
            Err(join_err) => log::error!("writer join error: {join_err}"),
        }
    }
    progress_handle.abort();

    let total = write_count.load(Ordering::Relaxed);
    let secs  = started.elapsed().as_secs_f64();
    log::info!(
        "done. {} rows -> Kinetica in {:.1}s = {:.0} rows/sec",
        total, secs, total as f64 / secs.max(0.001)
    );

    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Oracle reader (async)
// ---------------------------------------------------------------------------

async fn reader_task(
    tid: usize,
    cfg: Arc<Config>,
    sql_template: Arc<String>,
    tx: async_channel::Sender<Batch>,
    total: Arc<AtomicU64>,
) -> Result<()> {
    // One Connection per task. The crate states Connection is Send+Sync but
    // serializes operations internally, so multiple Connections == real parallelism.
    let conn = Connection::connect(&cfg.oracle.dsn, &cfg.oracle.user, &cfg.oracle.password)
        .await
        .with_context(|| format!("[reader-{tid}] Oracle connect failed"))?;

    // ORA_HASH(expr, max_bucket) returns 0..=max_bucket inclusive,
    // so for N tasks we want max_bucket = N - 1.
    let max_bucket = cfg.pipeline.threads.saturating_sub(1);
    let predicate = format!(
        "ORA_HASH({}, {}) = {}",
        cfg.oracle.partition_column, max_bucket, tid
    );
    let sql = sql_template.replace("{partition_clause}", &predicate);
    log::info!("[reader-{tid}] {sql}");

    // Initial query: returns the first chunk PLUS cursor_id+columns we'll
    // reuse on every subsequent fetch_more call.
    let mut result = conn
        .query(&sql, &[])
        .await
        .with_context(|| format!("[reader-{tid}] initial query failed"))?;

    let columns: Vec<ColumnInfo> = result.columns.clone();
    let cursor_id = result.cursor_id;

    let batch_size = cfg.pipeline.batch_size;
    let mut batch: Batch = Vec::with_capacity(batch_size);
    let mut local: u64 = 0;

    // Streaming loop: process current chunk, fetch next, repeat.
    loop {
        for row in &result.rows {
            let obj = row_to_json_object(row, &columns);
            batch.push(obj);
            local += 1;

            if batch.len() >= batch_size {
                // .await on send => natural backpressure when writers are slow.
                let full = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                tx.send(full)
                    .await
                    .map_err(|_| anyhow!("[reader-{tid}] all writers gone"))?;
            }
        }

        if !result.has_more_rows {
            break;
        }

        // Server-side cursor still open -- pull the next chunk.
        result = conn
            .fetch_more(cursor_id, &columns, cfg.oracle.fetch_size)
            .await
            .with_context(|| format!("[reader-{tid}] fetch_more failed"))?;
    }

    // Flush remainder.
    if !batch.is_empty() {
        tx.send(batch)
            .await
            .map_err(|_| anyhow!("[reader-{tid}] all writers gone"))?;
    }

    // Best-effort connection close (errors here are non-fatal).
    let _ = conn.close().await;

    total.fetch_add(local, Ordering::Relaxed);
    log::info!("[reader-{tid}] done, {local} rows");
    Ok(())
}

/// Build a flat JSON object {column_name: json_value} from one Oracle row.
fn row_to_json_object(row: &Row, columns: &[ColumnInfo]) -> JsonValue {
    let mut obj = Map::with_capacity(columns.len());
    for col in columns {
        let v = match row.get_by_name(&col.name) {
            Some(val) => ora_value_to_json(&val),
            None      => JsonValue::Null,
        };
        obj.insert(col.name.clone(), v);
    }
    JsonValue::Object(obj)
}

/// Convert an oracle_rs::Value into a serde_json::Value.
///
/// We try `serde_json::to_value` first because the crate is built on serde_json
/// (it even re-exports it) so Value almost certainly implements Serialize. If
/// your build of oracle-rs ever drops that impl, replace the body of this fn
/// with an explicit `match v { OraValue::Integer(i) => json!(i), ... }`.
fn ora_value_to_json(v: &OraValue) -> JsonValue {
    serde_json::to_value(v).unwrap_or(JsonValue::Null)
}

// ---------------------------------------------------------------------------
// Kinetica writer (async)
// ---------------------------------------------------------------------------

async fn writer_task(
    wid: usize,
    rx: async_channel::Receiver<Batch>,
    cfg: Arc<Config>,
    client: Client,
    total: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let endpoint = format!(
        "{}/insert/records/json",
        cfg.kinetica.url.trim_end_matches('/')
    );
    let mut local: u64 = 0;

    while let Ok(batch) = rx.recv().await {
        if stop.load(Ordering::Relaxed) {
            log::warn!("[writer-{wid}] stop flag set, dropping {} buffered rows", batch.len());
            continue;
        }

        let n = batch.len();

        // Kinetica's /insert/records/json takes `json_records` as an array
        // of JSON-encoded strings, NOT raw objects. Stringify each row.
        let json_records: Vec<String> = batch.iter().map(|v| v.to_string()).collect();
        let payload = json!({
            "table_name":   cfg.kinetica.table,
            "json_records": json_records,
            "options": {
                "update_on_existing_pk": "false",
                "return_record_ids":     "false"
            }
        });

        let mut backoff = Duration::from_millis(250);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let res = client
                .post(&endpoint)
                .basic_auth(&cfg.kinetica.user, Some(&cfg.kinetica.password))
                .json(&payload)
                .send()
                .await;

            match res {
                Ok(r) if r.status().is_success() => {
                    local += n as u64;
                    total.fetch_add(n as u64, Ordering::Relaxed);
                    break;
                }
                Ok(r) => {
                    let status = r.status();
                    let body   = r.text().await.unwrap_or_default();
                    if attempt >= cfg.pipeline.max_retries {
                        return Err(anyhow!(
                            "[writer-{wid}] giving up after {attempt} tries: {status} {body}"
                        ));
                    }
                    log::warn!("[writer-{wid}] attempt {attempt}: {status} -> retry in {backoff:?}");
                }
                Err(e) => {
                    if attempt >= cfg.pipeline.max_retries {
                        return Err(anyhow!("[writer-{wid}] giving up after {attempt} tries: {e}"));
                    }
                    log::warn!("[writer-{wid}] attempt {attempt}: {e} -> retry in {backoff:?}");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(8));
        }
    }

    log::info!("[writer-{wid}] done, {local} rows");
    Ok(())
}