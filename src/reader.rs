use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;
use std::sync::Arc;
use calamine::Sheets;
use csv::{ReaderBuilder, StringRecord};
use heck::ToSnakeCase;
use tokio::sync::mpsc;
use serde_json::{Number, Value};
use simple_string_patterns::*;
use indexmap::IndexMap;
use std::path::Path;

use calamine::{open_workbook_auto, Data, Reader};

use crate::fuzzy_datetime::correct_iso_datetime;
use crate::fuzzy_datetime::fuzzy_to_date_string;
use crate::fuzzy_datetime::fuzzy_to_datetime_string;
use crate::headers::*;
use crate::data_set::*;
use crate::helpers::float_value;
use crate::helpers::string_value;
use crate::is_truthy::*;
use crate::round_decimal::RoundDecimal;
use crate::Extension;
use crate::Format;
use crate::OptionSet;
use crate::euro_number_format::is_euro_number_format;
use crate::PathData;
use crate::RowOptionSet;
use crate::error::GenericError;

/// Output the result set with captured rows (up to the maximum allowed) directly.
/// This is now synchronous and calls the asynchronous function using a runtime.
pub fn process_spreadsheet_direct(opts: &OptionSet) -> Result<ResultSet, GenericError> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(process_spreadsheet_core(opts, None, None))
}

/// Output the result set with captured rows (up to the maximum allowed) immediately.
/// Use this in an async function using the tokio runtime if you direct results
/// without a save callback
pub async fn process_spreadsheet_immediate(opts: &OptionSet) -> Result<ResultSet, GenericError> {
  process_spreadsheet_core(opts, None, None).await
}

#[deprecated(
	since = "1.0.6",
	note = "This function is a wrapper for the renamed function `process_spreadsheet_inline`"
)]
pub async fn render_spreadsheet_direct(opts: &OptionSet) -> Result<ResultSet, GenericError> {
  process_spreadsheet_core(opts, None, None).await
}

/// Output the result set with deferred row saving and optional output reference
pub async fn process_spreadsheet_async(
  opts: &OptionSet,
  save_func: Box<dyn Fn(IndexMap<String, Value>) -> Result<(), GenericError> + Send + Sync>,
  out_ref: Option<&str>
  ) -> Result<ResultSet, GenericError> {
  process_spreadsheet_core(opts, Some(save_func), out_ref).await
}

/// Output the result set with captured rows (up to the maximum allowed) directly.
/// with optional asynchronous row save method and output reference
pub async fn process_spreadsheet_core(
    opts: &OptionSet,
    save_opt: Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), GenericError> + Send + Sync>>,
    out_ref: Option<&str>
) -> Result<ResultSet, GenericError> {
    if let Some(filepath) = opts.path.clone() {
        let path = Path::new(&filepath);
        if !path.exists() {
            #[allow(dead_code)]
            return Err(GenericError("file_unavailable"));
        }
        let path_data = PathData::new(path);
        if path_data.is_valid() {
            if path_data.use_calamine() {
                read_workbook_core(&path_data, opts, save_opt, out_ref).await
            } else {
                read_csv_core(&path_data, opts, save_opt, out_ref).await
            }
        } else {
            Err(GenericError("unsupported_format"))
        }
    } else {
        Err(GenericError("no_filepath_specified"))
    }
}


#[deprecated(
    since = "1.0.6",
    note = "This function is a wrapper for the renamed function `process_spreadsheet_core`"
)]
pub async fn render_spreadsheet_core(
    opts: &OptionSet,
    save_opt: Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), GenericError> + Send + Sync>>,
    out_ref: Option<&str>
) -> Result<ResultSet, GenericError> {
    process_spreadsheet_core(opts, save_opt, out_ref).await
}

/// Parse spreadsheets with an optional callback method to save rows asynchronously and an optional output reference
/// that may be a file name or database identifier
pub async fn read_workbook_core<'a>(
    path_data: &PathData<'a>,
    opts: &OptionSet,
    save_opt: Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), GenericError> + Send + Sync>>,
    out_ref: Option<&str>
) -> Result<ResultSet, GenericError> {
    if let Ok(mut workbook) = open_workbook_auto(path_data.path()) {
        let max_rows = opts.max_rows();
        let (selected_names, sheet_names, _sheet_indices) = match_sheet_name_and_index(&mut workbook, opts);
        

        if selected_names.len() > 0 {
            let info = WorkbookInfo::new(path_data, &selected_names, &sheet_names);

            if opts.multimode() {
                read_multiple_worksheets(&mut workbook, &sheet_names, opts, &info, max_rows).await
            } else {
                let sheet_ref = &selected_names[0];
                read_single_worksheet(workbook, sheet_ref, opts, &info, save_opt, out_ref).await
            }
        } else {
            Err(GenericError("workbook_with_no_sheets"))
        }
    } else {
        Err(GenericError("cannot_open_workbook"))
    }
}

async fn read_multiple_worksheets(
    workbook: &mut Sheets<BufReader<File>>,
    sheet_names: &[String],
    opts: &OptionSet,
    info: &WorkbookInfo,
    max_rows: usize
) -> Result<ResultSet, GenericError> {
    let mut sheets: Vec<SheetDataSet> = vec![];
    let mut sheet_index: usize = 0;
    let capture_rows = opts.capture_rows();
    for sheet_ref in sheet_names {
      let range = workbook.worksheet_range(&sheet_ref.clone())?;
      let mut headers: Vec<String> = vec![];
      let mut has_headers = false;
      let capture_headers = !opts.omit_header;
      let source_rows = range.rows();
      let mut rows: Vec<IndexMap<String, Value>> = vec![];
      let mut row_index = 0;
      let header_row_index = opts.header_row_index();
      let mut col_keys: Vec<String> = vec![];
      let columns = if sheet_index == 0 {
        opts.rows.columns.clone()
      } else {
        vec![]
      };
      let match_header_row_below = capture_headers && header_row_index > 0;
      if let Some(first_row) = range.headers() {
        
        headers = build_header_keys(&first_row, &columns, &opts.field_mode);
        has_headers = !match_header_row_below;
        col_keys = first_row;
      }
      let total = source_rows.clone().count();
      if capture_rows || match_header_row_below {
          let max_row_count = if capture_rows {
              max_rows
          } else {
              header_row_index + 2
          };
          let max_take = if total < max_row_count {
            total
          } else {
            max_row_count + 1
          };
          for row in source_rows.clone().take(max_take) {
              if row_index > max_row_count {
                  break;
              }
              if match_header_row_below && (row_index + 1) == header_row_index {
                  let h_row = row.into_iter().map(|c| c.to_string().to_snake_case()).collect::<Vec<String>>();
                  headers = build_header_keys(&h_row, &columns, &opts.field_mode);
                  has_headers = true;
              } else if (has_headers || !capture_headers) && capture_rows {
                  let row_map = workbook_row_to_map(row, &opts.rows, &headers);
                  if is_not_header_row(&row_map, row_index, &col_keys) {
                      rows.push(row_map);
                  }
              }
              row_index += 1;
          }
      }
      sheets.push(SheetDataSet::new(&sheet_ref, &headers, &rows, total));
      sheet_index += 1;
    }
    Ok(ResultSet::from_multiple(&sheets, &info))
}

pub async fn read_single_worksheet(
  mut workbook: Sheets<BufReader<File>>,
  sheet_ref: &str,
  opts: &OptionSet,
  info: &WorkbookInfo,
  save_opt: Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), GenericError> + Send + Sync>>,
  out_ref: Option<&str>,
) -> Result<ResultSet, GenericError> {
  let range = workbook.worksheet_range(sheet_ref)?;
  let capture_rows = opts.capture_rows();
  let columns = opts.rows.columns.clone();
  let max_rows = opts.max_rows();
  let mut headers: Vec<String> = vec![];
  let mut col_keys: Vec<String> = vec![];
  let mut has_headers = false;
  let capture_headers = !opts.omit_header;
  let source_rows = range.rows();
  let mut rows: Vec<IndexMap<String, Value>> = vec![];
  let mut row_index = 0;
  let header_row_index = opts.header_row_index();
  let match_header_row_below = capture_headers && header_row_index > 0;

  if let Some(first_row) = range.headers() {
      headers = build_header_keys(&first_row, &columns, &opts.field_mode);
      has_headers = !match_header_row_below;
      col_keys = first_row;
  }
  let total = source_rows.clone().count();
  if capture_rows || match_header_row_below {
      let max_row_count = if capture_rows {
          max_rows
      } else {
          header_row_index + 2
      };
      let max_take = if total < max_row_count {
        total
      } else {
        max_row_count + 1
      };
      for row in source_rows.clone().take(max_take) {
          if row_index > max_row_count {
              break;
          }
          if match_header_row_below && (row_index + 1) == header_row_index {
              let h_row = row.into_iter().map(|c| c.to_string().to_snake_case()).collect::<Vec<String>>();
              headers = build_header_keys(&h_row, &columns, &opts.field_mode);
              has_headers = true;
          } else if (has_headers || !capture_headers) && capture_rows {
              // only capture rows if headers are either omitted or have already been captured
              let row_map = workbook_row_to_map(row, &opts.rows, &headers);
              if is_not_header_row(&row_map, row_index,&col_keys) {
                rows.push(row_map);
              }
          }
          row_index += 1;
      }
  }
  if let Some(save_method) = save_opt {
      let (tx, mut rx) = mpsc::channel(32);
      let opts = Arc::new(opts.clone()); // Clone opts if possible, or wrap in Arc
      let headers = headers.clone();  
      let col_keys = col_keys.clone();   // Clone headers since it's used in the task
      let sheet_name = sheet_ref.to_string().clone();
      tokio::spawn(async move {
        if let Ok(range) = workbook.worksheet_range(&sheet_name) {
          let mut source_rows = range.rows();
          if let Some(first_row) = source_rows.next() {
            let first_row_map = workbook_row_to_map(&first_row, &opts.rows, &headers);
            // Send the first row
            if is_not_header_row(&first_row_map, 0, &col_keys) {
              if tx.send(first_row_map).await.is_err() {
                return;  // Early exit if the channel is closed
              }
            }
          }
  
          // Process the rest of the rows
          for row in source_rows {
              let row_map = workbook_row_to_map(&row, &opts.rows, &headers);
              if tx.send(row_map).await.is_err() {
                  break;  // Channel closed, stop sending
              }
          }
        }
      });
      // Process the rows as they come in
      while let Some(row) = rx.recv().await {
          save_method(row)?;
      }
  }
  
  let ds = DataSet::from_count_and_rows(total, rows, opts);
  Ok(ResultSet::new(info, &headers, ds, out_ref))
}

/// Process a CSV/TSV file asynchronously with an optional row save method 
/// and output reference (file or database table reference)
pub async fn read_csv_core<'a>(
    path_data: &PathData<'a>,
    opts: &OptionSet,
    save_opt: Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), GenericError> + Send + Sync>>,
    out_ref: Option<&str>
) -> Result<ResultSet, GenericError> {
    let separator = match path_data.mode() {
        Extension::Tsv => b't',
        _ => b',',
    };
    if let Ok(mut rdr) = ReaderBuilder::new().delimiter(separator).from_path(path_data.path()) {
        let capture_header = opts.omit_header == false;
        let mut rows: Vec<IndexMap<String, Value>> = vec![];
        let mut line_count = 0;
        let has_max = opts.max.is_some();

        let max_line_usize = opts.max_rows();
        let mut headers: Vec<String> = vec![];
        let capture_rows = opts.capture_rows();
        if capture_header {
            if let Ok(hdrs) = rdr.headers() {
                headers = hdrs.into_iter().map(|s| s.to_owned()).collect();
            }
            let columns = opts.rows.columns.clone();
            headers = build_header_keys(&headers, &columns, &opts.field_mode);
        }

        let mut total = 0;
        if capture_rows {
            for result in rdr.records() {
                if has_max && line_count >= max_line_usize {
                    break;
                }
                if let Some(row) = csv_row_result_to_values(result, Arc::new(&opts.rows)) {
                    rows.push(to_index_map(&row, &headers));
                    line_count += 1;
                }
            }
            total = line_count + rdr.records().count() + 1;
        } else {
            // duplicate reader for accurate non-consuming count
            if let Ok(mut count_rdr) = ReaderBuilder::new().from_path(&path_data.path()) {
                total = count_rdr.records().count();
            }
            // Spawn a task to read from CSV and save data row by row
            if let Some(save_method) = save_opt {
                let (tx, mut rx) = mpsc::channel(32);
                let opts = Arc::new(opts.clone()); // Clone opts if possible, or wrap in Arc
                let headers = headers.clone();     // Clone headers since it's used in the task
                tokio::spawn(async move {
                    for result in rdr.records() {
                        if let Some(row) = csv_row_result_to_values(result, Arc::new(&opts.rows)) {
                            let row_map = to_index_map(&row, &headers);
                            if tx.send(row_map).await.is_err() {
                                // Channel closed, stop sending
                                break;
                            }
                        }
                    }
                });

                // Process the rows as they come in
                while let Some(row) = rx.recv().await {
                    save_method(row)?;
                }
            }
        }
        let info = WorkbookInfo::simple(path_data);
        let ds = DataSet::from_count_and_rows(total, rows, opts);
        Ok(ResultSet::new(&info, &headers, ds, out_ref))
    } else {
        let error_msg = match path_data.ext() {
            Extension::Tsv => "unreadable_tsv_file",
            _ => "unreadable_csv_file"
        };
        Err(GenericError(error_msg))
    }
}

// Convert an array of row data to an IndexMap of serde_json::Value objects
fn workbook_row_to_map(row: &[Data], opts: &RowOptionSet, headers: &[String]) -> IndexMap<String, Value> {
    to_index_map(&workbook_row_to_values(row, &opts), headers)
}

// Convert an array of row data to a vector of serde_json::Value objects
fn workbook_row_to_values(row: &[Data], opts: &RowOptionSet) -> Vec<Value> {
    let mut c_index = 0;
    let mut cells: Vec<Value> = vec![];
    for cell in row {
        let value = workbook_cell_to_value(cell, Arc::new(opts), c_index);
        cells.push(value);
        c_index += 1;
    }
    cells
}

/// Convert a spreadsheet data cell to a polymorphic serde_json::Value object
fn workbook_cell_to_value(cell: &Data, opts: Arc<&RowOptionSet>, c_index: usize) -> Value {
    let col = opts.column(c_index);
    let format = col.map_or(Format::Auto, |c| c.format.to_owned());
    let def_val = col.and_then(|c| c.default.clone());

    match cell {
        Data::Int(i) => Value::Number(Number::from_i128(*i as i128).unwrap()),
        Data::Float(f) => process_float_value(*f, format),
        Data::DateTimeIso(d) => {
          Value::String(correct_iso_datetime(d))
        },
        Data::DateTime(d) => process_excel_datetime_value(d, def_val, opts.date_only),
        Data::Bool(b) => Value::Bool(*b),
        Data::String(s) => process_string_value(s, format, def_val),
        Data::Empty => def_val.unwrap_or(Value::Null),
        _ => Value::String(cell.to_string()),
    }
}

fn process_float_value(value: f64, format: Format) -> Value {
    match format {
        Format::Integer => Value::Number(Number::from_i128(value as i128).unwrap()),
        _ => Value::Number(Number::from_f64(value).unwrap()),
    }
}

fn process_excel_datetime_value(
    datetime: &calamine::ExcelDateTime,
    def_val: Option<Value>,
    date_only: bool
) -> Value {
    let dt_ref = datetime.as_datetime().map_or_else(
        || def_val.unwrap_or(Value::Null),
        |dt| Value::String(dt.format(if date_only { "%Y-%m-%d" } else { "%Y-%m-%dT%H:%M:%S" }).to_string())
    );
    dt_ref
}

fn process_string_value(value: &str, format: Format, def_val: Option<Value>) -> Value {
    match format {
        Format::Boolean => process_truthy_value(value, def_val, is_truthy_core),
        Format::Truthy => process_truthy_value(value, def_val, is_truthy_standard),
        Format::TruthyCustom(opts) => process_truthy_value(value, def_val, |v, _| is_truthy_custom(v, &opts, false, false)),
        Format::Decimal(places) => process_numeric_value(value, def_val, |n| float_value(n.round_decimal(places))),
        Format::Float => process_numeric_value(value, def_val, float_value),
        Format::Date => process_date_value(value, def_val, fuzzy_to_date_string),
        Format::DateTime => process_date_value(value, def_val, fuzzy_to_datetime_string),
        _ => Value::String(value.to_owned()),
    }
}

fn process_truthy_value<F>(value: &str, def_val: Option<Value>, truthy_fn: F) -> Value
where
    F: Fn(&str, bool) -> Option<bool>,
{
    if let Some(is_true) = truthy_fn(value, false) {
        Value::Bool(is_true)
    } else {
        def_val.unwrap_or(Value::Null)
    }
}

fn process_numeric_value<F>(value: &str, def_val: Option<Value>, numeric_fn: F) -> Value
where
    F: Fn(f64) -> Value,
{
    if let Some(n) = value.to_first_number::<f64>() {
        numeric_fn(n)
    } else {
        def_val.unwrap_or(Value::Null)
    }
}

fn process_date_value<F>(value: &str, def_val: Option<Value>, date_fn: F) -> Value
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(date_str) = date_fn(value) {
        string_value(&date_str)
    } else {
        def_val.unwrap_or(Value::Null)
    }
}

// Convert csv rows to value
fn csv_row_result_to_values(result: Result<StringRecord, csv::Error>, opts: Arc<&RowOptionSet>) -> Option<Vec<Value>> {
    if let Ok(record) = result {
        let mut row: Vec<Value> = vec![];
        let mut ci: usize = 0;
        for cell in record.into_iter() {
            let new_cell = csv_cell_to_json_value(cell, opts.clone(), ci);
            row.push(new_cell);
            ci += 1;
        }
        return Some(row)
    }
    None
}

// convert CSV cell &str value to a polymorphic serde_json::VALUE
fn csv_cell_to_json_value(cell: &str, opts: Arc<&RowOptionSet>, index: usize) -> Value {
    let has_number = cell.to_first_number::<f64>().is_some();
    // clean cell to check if it's numeric
    let col = opts.column(index);
    let fmt = if let Some(c) = col.cloned() {
        c.format
    } else {
        Format::Auto
    };
    let euro_num_mode = if let Some(c) = col.cloned() {
        c.decimal_comma
    } else {
        opts.decimal_comma
    };
    let num_cell = if has_number {
        let euro_num_mode = is_euro_number_format(cell, euro_num_mode);
        if euro_num_mode {
            cell.replace(",", ".").replace(",", ".")
        } else {
            cell.replace(",", "")
        }
    } else {
        cell.to_owned()
    };
    let mut new_cell = Value::Null;
    if num_cell.len() > 0 && num_cell.is_numeric() {
        if let Ok(float_val) = serde_json::Number::from_str(&num_cell) {
            match fmt {
                Format::Integer => {
                    if let Some(int_val) = Number::from_i128(float_val.as_i128().unwrap_or(0)) {
                        new_cell = Value::Number(int_val);
                    }
                },
                Format::Boolean => {
                    // only 1.0 or more will evaluate as true
                    new_cell = Value::Bool(float_val.as_f64().unwrap_or(0f64) >= 1.0);
                },
                _ => {
                    new_cell = Value::Number(float_val);
                }
            }
        }
    } else if let Some(is_true) = is_truthy_core(cell, false) {
        new_cell = Value::Bool(is_true);
    } else {
        new_cell = match fmt {
            Format::Truthy => {
                if let Some(is_true) = is_truthy_standard(cell, false) {
                    Value::Bool(is_true)
                } else {
                    Value::Null
                }
            }
            _ => Value::String(cell.to_string())
        };
    }
    new_cell
}



#[cfg(test)]
mod tests {
  use serde_json::json;

use crate::{helpers::*, Column};

use super::*;

  #[test]
  fn test_direct_processing_xlsx() {
      let sample_path = "data/sample-data-1.xlsx";

      // instantiate the OptionSet with a sample path and a maximum row count of 1000 rows as the source file has 401 rows
      // (although )the default max is 10,000)
      let opts = OptionSet::new(sample_path).max_row_count(1_000);

      let result = process_spreadsheet_direct(&opts); 
      
      // The source file should have 1 header row and 400 data rows
      assert_eq!(result.unwrap().num_rows,401);
  }

  #[test]
  fn test_direct_processing_csv() {
    let sample_path = "data/sample-data-1.csv";

    // instantiate the OptionSet with a sample path and a maximum row count of 1000 rows as the source file has 401 rows
    // (although )the default max is 10,000)
    let opts = OptionSet::new(sample_path).max_row_count(1_000);

    let result = process_spreadsheet_direct(&opts);
    
    // The source file should have 1 header row and 400 data rows
    assert_eq!(result.unwrap().num_rows,401);
  }

  #[test]
  fn test_multisheet_preview_ods() {
    let sample_path = "data/sample-data-2.ods";

    // instantiate the OptionSet with a sample path
    // a maximum row count returned of 10 rows
    // and read mode to *preview* to scan all sheets
    // It should correctly calculate
    let opts = OptionSet::new(sample_path)
      .max_row_count(10)
      .read_mode_preview();

    let result = process_spreadsheet_direct(&opts);
    
    // The source spreadsheet should have 2 sheets
    let dataset = result.unwrap();
    assert_eq!(dataset.sheets.len(),2);
    // The source spreadsheet should have 101 + 17 (= 118) populated rows including headers
    assert_eq!(dataset.num_rows,118);

    // The first sheet's data should only output 10 rows (including the header)
    assert_eq!(dataset.data.first_sheet().len(), 10);
  }

  #[test]
  fn test_column_override_1() {
    let sample_json = json!({
      "sku": "CHAIR16",
      "height": "112cm",
      "width": "69cm",
      "approved": "Y"
    });

    let rows = json_object_to_calamine_data(sample_json);

    let cols = vec![
        Column::new_format(Format::Text, Some(string_value(""))),
        Column::new_format(Format::Float, Some(float_value(95.0))),
        Column::new_format(Format::Float, Some(float_value(65.0))),
        Column::new_format(Format::Truthy, Some(bool_value(false))),
    ];

    // The first sheet's data should only output 10 rows (including the header)
    let opts = &RowOptionSet::simple(&cols);
    let result =  workbook_row_to_values(&rows, opts);
    // the second column be cast to 112.0
    assert_eq!(result.get(1).unwrap(), 112.0);
    // the third column be cast to 69.0
    assert_eq!(result.get(2).unwrap(), 69.0);
    // the fourth column be cast to boolean
    assert_eq!(result.get(3).unwrap(), true);
  }


  #[test]
  fn test_column_override_2() {
    let sample_json = json!({
      "name": "Sophia",
      "dob": "2001-9-23",
      "weight": "62kg",
      "result": "GOOD"
    });

    let rows = json_object_to_calamine_data(sample_json);

    let cols = vec![
        Column::new_format(Format::Text, None),
        Column::new_format(Format::Date, None),
        Column::new_format(Format::Float, None),
        // the fourth column be cast to boolean
        Column::new_format(Format::truthy_custom("good", "bad"), Some(bool_value(false))),
    ];

    // The first sheet's data should only output 10 rows (including the header)
    let opts = &RowOptionSet::simple(&cols);
    let result =  workbook_row_to_values(&rows, opts);
    assert_eq!(result.get(1).unwrap(), "2001-09-23");
    assert_eq!(result.get(2).unwrap(), 62.0);
    assert_eq!(result.get(3).unwrap(), true);
  }

}