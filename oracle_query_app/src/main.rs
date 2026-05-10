use oracle::{Connection, Result};
use clap::Parser;

use arrow::array::{StringArray, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

#[derive(Parser, Debug)]
#[command(version, about = "Query Oracle Database and write results to Parquet files in parallel.")]
struct Args {
    #[arg(short = 'd', long, help = "Output directory for Parquet files")]
    output_dir: String,

    #[arg(short = 'm', long, default_value = "50000", help = "Maximum records per Parquet file")]
    max_records: usize,

    #[arg(short = 't', long, default_value = "4", help = "Number of parallel threads")]
    threads: usize,
}

const USERNAME: &str = "appuser";
const PASSWORD: &str = "appuserpass";
const CONNECT_STRING: &str = "//localhost:1521/FREEPDB1";
const PRODUCT_TYPE: &str = "LOAN";

fn main() -> Result<()> {
    let args = Args::parse();

    std::fs::create_dir_all(&args.output_dir).expect("Failed to create output directory");

    let conn = Connection::connect(USERNAME, PASSWORD, CONNECT_STRING)?;
    let total_rows: i64 = conn.query_row_as(
        "SELECT COUNT(*) FROM BALANCE_FORECAST_MODEL WHERE PRODUCT_TYPE = :1",
        &[&PRODUCT_TYPE],
    )?;
    println!("Total rows: {}", total_rows);

    if total_rows == 0 {
        println!("No rows found.");
        return Ok(());
    }

    let total_rows = total_rows as usize;
    let num_threads = args.threads.min(total_rows);
    let chunk_size = (total_rows + num_threads - 1) / num_threads;

    let output_dir = Arc::new(args.output_dir.clone());
    let file_counter = Arc::new(AtomicUsize::new(0));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_idx| {
            let output_dir = Arc::clone(&output_dir);
            let file_counter = Arc::clone(&file_counter);
            let max_records = args.max_records;
            let offset = (thread_idx * chunk_size) as i64;
            let limit = chunk_size.min(total_rows - thread_idx * chunk_size) as i64;
            let core_id = if !core_ids.is_empty() {
                core_ids.get(thread_idx % core_ids.len()).copied()
            } else {
                None
            };

            thread::spawn(move || {
                if let Some(id) = core_id {
                    core_affinity::set_for_current(id);
                }

                println!("Thread {}: querying offset={} limit={} (core={:?})", thread_idx, offset, limit, core_id);

                let conn = Connection::connect(USERNAME, PASSWORD, CONNECT_STRING)
                    .expect("DB connect failed");

                let sql = "SELECT * FROM BALANCE_FORECAST_MODEL \
                           WHERE PRODUCT_TYPE = :product_type \
                           ORDER BY ROWID \
                           OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY";

                let rows: Vec<oracle::Row> = conn
                    .query(sql, &[&PRODUCT_TYPE, &offset, &limit])
                    .expect("Query failed")
                    .collect::<oracle::Result<Vec<_>>>()
                    .expect("Row collection failed");

                if rows.is_empty() {
                    return;
                }

                let col_infos = rows[0].column_info();
                let num_cols = col_infos.len();
                let col_names: Vec<String> = col_infos.iter().map(|c| c.name().to_string()).collect();

                let fields: Vec<Field> = col_names.iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect();
                let schema = Arc::new(Schema::new(fields));

                for chunk in rows.chunks(max_records) {
                    let mut columns: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(chunk.len()); num_cols];
                    for row in chunk {
                        for i in 0..num_cols {
                            columns[i].push(row.get::<_, Option<String>>(i).unwrap_or(None));
                        }
                    }

                    let arrays: Vec<ArrayRef> = columns.into_iter()
                        .map(|col| Arc::new(StringArray::from(col)) as ArrayRef)
                        .collect();

                    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
                        .expect("Failed to create RecordBatch");

                    let file_num = file_counter.fetch_add(1, Ordering::SeqCst);
                    let file_path = format!("{}/part_{:06}.parquet", output_dir, file_num);

                    write_parquet_file(&file_path, batch).expect("Failed to write Parquet file");
                    println!("Thread {}: wrote {} ({} records)", thread_idx, file_path, chunk.len());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    println!("Done. Files written to: {}", args.output_dir);
    Ok(())
}

fn write_parquet_file(file_path: &str, record_batch: RecordBatch) -> parquet::errors::Result<()> {
    let file = File::create(file_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props))?;
    writer.write(&record_batch)?;
    writer.close()?;
    Ok(())
}
