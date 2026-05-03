use oracle::{Connection, Result};
use prettytable::{Cell, Row, Table};
use clap::Parser;

use arrow::array::{StringArray, Float64Array, ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(version, about = "A Rust application to query Oracle Database and display results in a table format.")]
struct Args {
    #[arg(short, long, help = "Output file path for the Parquet file")]
    output: String,
}

fn main() -> Result<()> {

    //1. Connect to Oracle Database
    let username = "appuser";
    let password = "appuserpass";
    let connect_string = "//localhost:1521/FREEPDB1"; // Adjust as



    // Establish a connection to the Oracle database
    let conn = Connection::connect(username, password, connect_string)?;

    //Run a Select query with bind parameters
    let sql = "SELECT * FROM BALANCE_FORECAST_MODEL
                        WHERE PRODUCT_TYPE = :product_type
                        AND ROWNUM <= 10"; // Limit to 10 rows for demonstration

    let product_type = "LOAN"; // Replace with the actual product type

    //Collect into Vec
    let rows: Vec<oracle::Row> = conn.query(sql, &[&product_type])?.collect::<Result<Vec<_>>>()?;

    println!("Query executed successfully. Number of rows returned: {}", rows.len());

    // Create a table to display the results
    let mut table = Table::new();
    table.add_row(Row::new(vec![
        Cell::new("ENTITY_ID"),
        Cell::new("BUSINESS_UNIT"),
        Cell::new("PRODUCT_TYPE"),
        Cell::new("PRODUCT_SUBTYPE"),
        Cell::new("CURRENCY_CODE"),
        Cell::new("CUSTOMER_SEGMENT"),

    ]));

    // Iterate through the result set and add rows to the table
    for row in &rows {
        let entity_id: String = row.get(0)?;
        let business_unit: String = row.get(1)?;
        let product_type: String = row.get(2)?;
        let product_subtype: String = row.get(3)?;
        let curreny_code: String = row.get(4)?;
        let customer_segment: String = row.get(5)?;

        table.add_row(Row::new(vec![
            Cell::new(&entity_id),
            Cell::new(&business_unit),
            Cell::new(&product_type),
            Cell::new(&product_subtype),
            Cell::new(&curreny_code),
            Cell::new(&customer_segment),
        ]));
    }

    // Print the table
    table.printstd();

    Ok(())
}

fn write_parquet_file(file_path: &str, record_batch: RecordBatch) -> parquet::errors::Result<()> {
    let file = File::create(file_path)?;
    let schema = record_batch.schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    writer.write(&record_batch)?;
    writer.close()?;
    Ok(())
}
