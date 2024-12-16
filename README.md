# spreadsheet-to-json: Convert Spreadsheets and CSV files to jSON


This library crate provides the core functions to convert common spreadsheet and CSV files into JSON or JSONL (JSON Lines) either directly or asynchronously.

It relies on the [Calamine](https://crates.io/crates/calamine) and [CSV](https://crates.io/crates/csv) library crates to process files, the [tokio](https://crates.io/crates/tokio) crate for asynchronous operations and naturally [serde](https://crates.io/crates/serde) and [serde_json](https://crates.io/crates/serde_json) serialization libraries.

## Examples

### Simple immediate parsing. This must be called in an async function.
```rust
use spreadsheet_to_json::*;
use spreadsheet_to_json::tokio;

#[tokio:main]
async fn main() -> Result((), Error) {
  let opts = Opts::new("path/to/spreadsheet.xslx")->set_sheet_index(1);
  let result = render_spreadsheet_direct(&opts).await;
  let json_value = match result {
    Err(msg_code) => json!{ { "error": true, "key": msg_code.to_string() },
    Ok(data_set) => data_set.to_json() // full result set
  };
  println!("{}", json_value);
}
```


### Asynchronous parsing. This must be called in an async function.
```rust
use spreadsheet_to_json::*;
use spreadsheet_to_json::tokio;
use spreadsheet_to_json::calamine::Error;

async fn main() -> Result((), Error) {
  let opts = Opts::new("path/to/spreadsheet.xslx")->read_mode_async();
  let dataset_id = db_dataset_id(&opts);

  fn save_data_row(row: IndexMap<String, Value>) -> Result((), Error) {
    
  }

  let result = render_spreadsheet_core(&opts, Some(save_data_row), Some(dataset_id)).await;
  let result_set = match result {
      Err(msg_code) => json!{ { "error": true, "key": msg_code.to_string() },
      Ok(data_set) => data_set.to_json() // full result set
  };
  println!("{}", result_set);
}
```


