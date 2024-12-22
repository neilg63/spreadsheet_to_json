[![mirror](https://img.shields.io/badge/mirror-github-blue)](https://github.com/neilg63/spreadsheet-to-json)
[![crates.io](https://img.shields.io/crates/v/spreadsheet-to-json.svg)](https://crates.io/crates/spreadsheet-to-json)
[![docs.rs](https://docs.rs/spreadsheet-to-json/badge.svg)](https://docs.rs/spreadsheet-to-json)

# spreadsheet-to-json

## Convert Spreadsheets and CSV files to jSON

### NB: THis is still in alpha!!!

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
- Blazingly fast: It can import 10,000 rows in less than 0.4 seconds.
- Can export to standard JSON or to JSON lines when writing large files
- Formula cells are read as calculated values
- Can identify and convert both Excel's 1900 datetime format and standard ISO format as used in OpenDocument Spreadsheet
- Can identify numeric fields formatted as text and convert them to integers or floats.
- Can identify truthy text or number cells and convert them to booleans
- Can save large files asynchronously

## To do
Full explanation of options to come.

## Core Options

Options can be set by instantiating `OptionSet::new("path/to/spreadsheet.xlsx")` with chained setter methods:

- `.max_row_count(max: u32)`: overrides the default max row count of 10,000. Use this is direct mode or to return only the first *n* rows.
- `.header_row(index: u8)` overrides the default header row index of 0, useful for spreadsheets with a title and notes on top
- `.omit_header(index: u8)` omit the header altogether and assign default *A1-style* keys or column numbers.
- `.sheet_index(index: u32)` zero-based sheer index. Any value over zero will override the specified sheet name.
- `.sheet_name(name: &str)` case-insensitive sheet name. It will match the first sheet with name after stripping spaces and punctuation.
- `.read_mode_async()` Defer processing of rows with a callback in the second argument in render_spreadsheet_async() 
- `.json_lines()` Output will be rendered one json object per row.

- `field_name_mode(system: &str, override_header: bool)`: use either A1 or C for the default column key notation where headers are either unavailable or suppressed via the `override_header` flag.
- `override_headers(keys: &[&str])` Override matched or automatic column keys. More advanced column options will be detailed soon.
- `override_columns(cols: &[Value])` This lets you override column key names and value formats via a hashmap, represented here as a serde_json::Value`. More details to come.

Simple example:
```rust 
  let opts = OptionsSet::new("path/to/spreadsheet.ods")
      .sheet_index(3)
      .read_mode_async()
      .override_headers(&["name", "height", "width", "weight", "hue", "price"]);
```

## Core functions

- `process_spreadsheet_direct(opts: &OptionSet)`: May be called in a synchronous context where you need to process results immediately.

- `process_spreadsheet_async(opts: &OptionSet)`: Asynchronously processes files with a callback function to save each row.

## Result set

- `filename`: Matched filename,
- `extension`: Matched extension
- `sheet`: Matched worksheet name and index
- `sheets`: List of available worksheet names
- `keys`: Assigned column keys
- `num_rows`: number of rows in the source file that have been successfully parsed
- `data`: Vector of dynamic objects (IndexMap<String, Value>) that can be easily translated into JSON or other common formats.
- `out_ref`: Optional output reference such as a generated file name, URL or database id.

If the file name and extension cannot be matched, because the file is unavailable or unsupported, the core functions will return a generic error.

### Result Set methods

- `to_json()`: Converts to the result set to `serde_json::Value` that may be printed directly or written to a file.
- `to_output_lines(json_lines: bool)`: Returns a vector of plain-text results with each data row as JSON on a new line
- `rows()`: Returns a vector of rendered JSON strings
- `json_rows(json_lines: bool)`: Returns all data rows as `serde_json::Value::Array` ready for conversion

## Alpha Version History
This crate is still alpha and likely to undergo breaking changes as it's part of larger data import project. I do not expect a stable version before mid January when it has been battle-tested.
- **0.1.2** the core public functions with *Result* return types now use a GenericError error type
- **0.1.3** Refined A1 and C01 column name styles and added result output as vectors of lines for interoperability with CLI utilities and debugging.
- **0.1.4** Added support for Excel Binary format (.xlsb)
- **0.1.5** Added two new core functions `process_spreadsheet_direct()` for direct row processing in a synchronous context and `process_spreadsheet_direct()`  in an asynchronous context with a callback. If you need to process a spreadsheet directly in an async function
- **0.1.6** Deprecated public function beginning with render (render_spreadsheet_direct() has become you should use `process_spreadsheet_immediate()` for immediate processing of spreadsheets in an async context). Ensured the header row does not appear as the first data row in spreadsheets.

## Examples

The main implementation is my unpublished [Spreadsheet to JSON CLI](https://github.com/neilg63/spreadsheet_to_json_cli) crate,

### Simple immediate jSON conversion

This function processes the spreadsheet file immediately. 

```rust
use spreadsheet_to_json::*;

fn main() -> Result((), Error) {
  let opts = Opts::new("path/to/spreadsheet.xlsx")->set_sheet_index(1);
  let result = process_spreadsheet_direct(&opts);
  let json_value = match result {
    Err(msg_code) => json!{ { "error": true, "key": msg_code.to_string() },
    Ok(data_set) => data_set.to_json() // full result set
  };
  println!("{}", json_value);
}
```


### Asynchronous parsing saving to a database

This must be called in an async function and save rows in separate proc
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

  let result = process_spreadsheet_async(&opts, callback, Some(dataset_id)).await;
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


