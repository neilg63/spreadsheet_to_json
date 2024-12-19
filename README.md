[![mirror](https://img.shields.io/badge/mirror-github-blue)](https://github.com/neilg63/spreadsheet-to-json)
[![crates.io](https://img.shields.io/crates/v/spreadsheet-to-json.svg)](https://crates.io/crates/spreadsheet-to-json)
[![docs.rs](https://docs.rs/spreadsheet-to-json/badge.svg)](https://docs.rs/spreadsheet-to-json)

# spreadsheet-to-json

## Convert Spreadsheets and CSV files to jSON

This library crate provides the core functions to convert common spreadsheet and CSV files into JSON or JSONL (JSON Lines) either directly or asynchronously.

It relies on the [Calamine](https://crates.io/crates/calamine) and [CSV](https://crates.io/crates/csv) library crates to process files, the [tokio](https://crates.io/crates/tokio) crate for asynchronous operations and naturally [serde](https://crates.io/crates/serde) and [serde_json](https://crates.io/crates/serde_json) serialization libraries.

It supports the following formats:

- Excel 2007+ Workbook (*.xlsx*)
- Excel 2007+ Binary (*.xlsb*)
- Excel 97-2004 Legacy (*.xls*)
- OpenDocument Spreadsheets (*.ods*) compatible with LibreOffice
- CSV: comma separated values (*.csv*)
- TSV: tab-separated values (*.tsv*)

## Features
- Blazingly fast. It can import 10,000 rows in 0.4 seconds.
- Can export to standard JSON or to JSON lines when writing large files
- Formula cells are read as calculated values
- Can identify and convert both Excel's 1900 datetime format and standard ISO format as used in OpenDocument Spreadsheet
- Can identify numeric fields formatted as text and convert them to integers or floats.
- Can identify truthy text or number cells and convert them to booleans
- Can save large files asynchronously

## To do
Full explanation of options to come.

## Alpha warning
This crate is still alpha and likely to undergo breaking changes as it's part of larger data import project. I do not expect a stable version before mid January when it has been battle-tested.
- **0.1.2** the core public functions with *Result* return types now use a GenericError error type
- **0.1.3** Refined A1 and C01 column name styles and added result output as vectors of lines for interoperability with CLI utilities and debugging.
- **0.1.4** Added support for Excel Binary format (.xlsb)
## Examples

The main implementation is my unpublished [Spreadsheet to JSON CLI](https://github.com/neilg63/spreadsheet_to_json_cli) crate,

### Simple immediate jSON conversion

This must be called in an async function.

```rust
use spreadsheet_to_json::*;
use spreadsheet_to_json::tokio;

#[tokio:main]
async fn main() -> Result((), Error) {
  let opts = Opts::new("path/to/spreadsheet.xlsx")->set_sheet_index(1);
  let result = render_spreadsheet_direct(&opts).await;
  let json_value = match result {
    Err(msg_code) => json!{ { "error": true, "key": msg_code.to_string() },
    Ok(data_set) => data_set.to_json() // full result set
  };
  println!("{}", json_value);
}
```


### Asynchronous parsing saving to a database

This must be called in an async function.
```rust
use spreadsheet_to_json::*;
use spreadsheet_to_json::tokio;

#[tokio:main]
async fn main() -> Result((), GenericError) {
  let opts = Opts::new("path/to/spreadsheet.xlsx")->read_mode_async();
  let dataset_id = db_dataset_id(&opts);

  let callback = move |row: IndexMap<String, Value>| -> Result<(), Error> {
    save_data_row(row, &connection, dataset_id)
  };

  let result = render_spreadsheet_core(&opts, Some(callback), Some(dataset_id)).await;
  let result_set = match result {
      Err(msg_code) => json!{ { "error": true, "key": msg_code.to_string() },
      Ok(data_set) => data_set.to_json() // full result set
  };
  println!("{}", result_set);
}

// Save function called in a closure for each row with a database connection and data_id from the outer scope
fn save_data_row(row: IndexMap<String, Value>, connection: PgConnection, data_id: u32) -> Result((), GenericError) {
    let mut row_struct = CustomTableStruct {
    id: None,
    data_id: data_id, // or whatever ID setting logic you have
    field1: None,
    field2: None,
    // ... set other fields to None or default values
  };
  
  for (key, value) in row {
    match key.as_str() {
        "field1" => {
            if let Value::String(s) = value {
                row_struct.field1 = Some(s.clone());
            }
        },
        "field2" => {
            if let Value::Number(n) = value {
                if let Some(i) = n.as_i64() {
                    row_struct.field2 = Some(i as i32);
                }
            }
        },
        // Add other field mappings here
        _ => {} // Ignore unknown keys
    }
  }
  diesel::insert_into("data_rows")
  .values(&row_struct)
  .execute(connection),map_error(|_| GenericError("database_error"));
  Ok()
}
```


