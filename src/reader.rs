use std::str::FromStr;
use std::sync::Arc;

use csv::ReaderBuilder;
use csv::StringRecord;
use heck::ToSnakeCase;
use tokio::sync::mpsc;
use serde_json::{Number, Value};
use simple_string_patterns::*;
use indexmap::IndexMap;
use std::path::Path;

use calamine::{open_workbook_auto, Data, Error,Reader};

use crate::headers::*;
use crate::data_set::*;
use crate::is_truthy::is_truthy_core;
use crate::Extension;
use crate::Format;
use crate::OptionSet;
use crate::euro_number_format::is_euro_number_format;
use crate::PathData;
use crate::RowOptionSet;

/* pub fn render_spreadsheet(opts: &OptionSet) -> Result<ResultSet, Error> {
    
    if let Some(filepath) = opts.path.clone() {
        let path = Path::new(&filepath);
        if !path.exists() {
            let canonical_path = path.canonicalize()?;
            let fpath = canonical_path.to_str().unwrap_or("");
            return Err(From::from("The file $fpath is not available"));
        }
        let path_data = PathData::new(path);
        if path_data.is_valid() {
          if path_data.use_calamine() {
            read_workbook(&path_data, opts)
          } else {
            read_csv(&path_data, opts)
          }
        } else {
          Err(From::from("Unsupported format"))
        }
    } else {
        Err(From::from("No file path specified"))
    }
} */

pub async fn render_spreadsheet_direct(
  opts: &OptionSet) -> Result<ResultSet, Error> {  
  render_spreadsheet_core(opts, None, None).await
}

pub async fn render_spreadsheet_core(
    opts: &OptionSet,
    save_opt:  Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), Error> + Send + Sync>>,
    out_ref: Option<&str>
  ) -> Result<ResultSet, Error> {
    
  if let Some(filepath) = opts.path.clone() {
      let path = Path::new(&filepath);
      if !path.exists() {
          let canonical_path = path.canonicalize()?;
          let fpath = canonical_path.to_str().unwrap_or("");
          return Err(From::from("The file $fpath is not available"));
      }
      let path_data = PathData::new(path);
      if path_data.is_valid() {
        if path_data.use_calamine() {
          read_workbook_core(&path_data, opts, save_opt, out_ref).await
        } else {
          read_csv_core(&path_data, opts, save_opt, out_ref).await
        }
      } else {
        Err(From::from("Unsupported format"))
      }
  } else {
      Err(From::from("No file path specified"))
  }
}

/* 
pub fn read_workbook(path_data: &PathData, opts: &OptionSet) -> Result<ResultSet, Error> {

    if let Ok(mut workbook) = open_workbook_auto(path_data.path()) {
      let columns = opts.rows.columns.clone();
      let max_rows = opts.max_rows();
        let (sheet_name_opt, sheet_names, sheet_index) = match_sheet_name_and_index(&mut workbook, opts);
        if let Some(first_sheet_name) = sheet_name_opt {
            let range = workbook.worksheet_range(&first_sheet_name)?;
            let mut headers: Vec<String> = vec![];
            let mut has_headers = false;
            let capture_headers = !opts.omit_header;
            let source_rows  = range.rows();
            let mut sheet_map: Vec<IndexMap<String, Value>> = vec![];
            let mut row_index = 0;
            let header_row_index = opts.header_row_index();
            let match_header_row_below = capture_headers && header_row_index > 0;

            if let Some(first_row) = range.headers() {
                headers = build_header_keys(&first_row, &columns);
                has_headers = !match_header_row_below;
            }
            let total = source_rows.clone().count();
            for row in source_rows {
              if row_index >= max_rows {
                break;
              }
              if match_header_row_below && (row_index + 1) == header_row_index {
                let h_row = row.into_iter().map(|c| c.to_string().to_snake_case()).collect::<Vec<String>>();
                headers = build_header_keys(&h_row, &columns);
                has_headers = true;
              } else if has_headers || !capture_headers {
                // only capture rows if headers are either ommitted or have already been captured
                let cells = workbook_row_to_values(row, &opts.rows);
                sheet_map.push(to_index_map(&cells, &headers));
              }
              row_index += 1;
            }
            let info = WorkbookInfo::new(path_data, &first_sheet_name, sheet_index, &sheet_names);
            Ok(ResultSet::new(info, &headers, DataSet::WithRows(total, sheet_map), None))
        } else {
            Err(From::from("the workbook does not have any sheets"))
        }
    }  else {
        Err(From::from("Cannot open the workbook"))
    }
}
 */

// Supports asynchronous handling of large spreadsheets
pub async fn read_workbook_core<'a>(
    path_data: &PathData<'a>, opts: &OptionSet,
    save_opt:  Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), Error> + Send + Sync>>,
    out_ref: Option<&str>
  )  -> Result<ResultSet, Error> {
  if let Ok(mut workbook) = open_workbook_auto(path_data.path()) {
    let columns = opts.rows.columns.clone();
    let max_rows = opts.max_rows();
    let (sheet_name_opt, sheet_names, sheet_index) = match_sheet_name_and_index(&mut workbook, opts);
    let capture_rows = opts.capture_rows();
    println!("cr: {}", capture_rows);
    if let Some(first_sheet_name) = sheet_name_opt {
      let range = workbook.worksheet_range(&first_sheet_name.clone())?;
      let mut headers: Vec<String> = vec![];
      let mut has_headers = false;
      let capture_headers = !opts.omit_header;
      let source_rows  = range.rows();
      let mut rows: Vec<IndexMap<String, Value>> = vec![];
      let mut row_index = 0;
      let header_row_index = opts.header_row_index();
      let match_header_row_below = capture_headers && header_row_index > 0;

      if let Some(first_row) = range.headers() {
        headers = build_header_keys(&first_row, &columns);
        has_headers = !match_header_row_below;
      }
      let total = source_rows.clone().count();
      if capture_rows || match_header_row_below {
        let max_row_count = if capture_rows {
          max_rows
        } else {
          header_row_index + 2
        };
        
        for row in source_rows.clone().take(max_row_count).collect::<Vec<&[Data]>>() {
          if row_index >= max_row_count {
            break;
          }
          if match_header_row_below && (row_index + 1) == header_row_index {
            let h_row= row.into_iter().map(|c| c.to_string().to_snake_case()).collect::<Vec<String>>();
            headers = build_header_keys(&h_row, &columns);
            has_headers = true;
          } else if (has_headers || !capture_headers) && capture_rows{
            // only capture rows if headers are either omitted or have already been captured
            let row_map = workbook_row_to_map(row, &opts.rows, &headers);
            rows.push(row_map);
          }
          row_index += 1;
        }
      }
      if let Some(save_method) = save_opt {
        let (tx, mut rx) = mpsc::channel(32);
        let opts = Arc::new(opts.clone()); // Clone opts if possible, or wrap in Arc
        let headers = headers.clone();     // Clone headers since it's used in the task
        let first_sheet_name_clone = first_sheet_name.clone();
        tokio::spawn(async move {
          if let Ok(range) = workbook.worksheet_range(&first_sheet_name_clone) {
            let source_rows  = range.rows();
            for row in source_rows {
              let row_map = workbook_row_to_map(row, &opts.rows, &headers);
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
      let info = WorkbookInfo::new(path_data, &first_sheet_name, sheet_index, &sheet_names);
      let ds = DataSet::from_count_and_rows(total, rows, opts.read_mode());
      Ok(ResultSet::new(info, &headers, ds, out_ref))
    } else {
      Err(From::from("the workbook does not have any sheets"))
    }
  } else {
    Err(From::from("Cannot open the workbook"))
  }
}
  

fn workbook_row_to_map(row: &[Data], opts: &RowOptionSet,  headers: &[String]) ->  IndexMap<String, Value> {
  to_index_map(&workbook_row_to_values(row, &opts), headers)
}

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

fn workbook_cell_to_value(cell:&Data, opts: Arc<&RowOptionSet>, c_index: usize) -> Value {
  let col = opts.column(c_index);
  let format = if let Some(c) = col {
    c.format.to_owned()
  } else {
    Format::Auto
  };
  match cell {
    Data::Int(i) => Value::Number(Number::from_i128(*i as i128).unwrap()),
    Data::Float(f) => {
      match format {
        Format::Integer => Value::Number(Number::from_i128(*f as i128).unwrap()),
        _ => Value::Number(Number::from_f64(*f).unwrap())
      }
    },
    Data::DateTimeIso(d) => Value::String(d.to_owned()),
    Data::DateTime(d) => {
        let ndt = d.as_datetime();
        let dt_ref = if let Some(dt) = ndt {
            let fmt_str = match format {
              Format::Date => "%Y-%m-%d",
              _ => if opts.date_only {
                "%Y-%m-%d"
              } else {
                "%Y-%m-%dT%H:%M:%S.000Z"
              }
            };
            dt.format(fmt_str).to_string()
        } else {
            "".to_string()
        };
        Value::String(dt_ref)
    },
    Data::Bool(b) => Value::Bool(*b),
    // For other types, convert to string since JSON can't directly represent them as unquoted values
    Data::Empty => Value::Null,
    _ => Value::String(cell.to_string())
  }
}
/* 
pub fn read_csv(path_data: &PathData, opts: &OptionSet) -> Result<ResultSet, Error> {
    if let Ok(mut rdr)= ReaderBuilder::new().from_path(path_data.path()) {
      let capture_header = opts.omit_header == false;
      let mut rows: Vec<IndexMap<String, Value>> = vec![];
      let mut line_count = 0;
      let has_max = opts.max.is_some();
      let max_line_usize = opts.max_rows();
      let mut headers: Vec<String> = vec![];
      // let mut has_headers = false;
      if capture_header {
        if let Ok(hdrs) = rdr.headers() {
            headers = hdrs.into_iter().map(|s| s.to_owned()).collect();
            // has_headers = true;
        }
        let columns = opts.columns();
        headers = build_header_keys(&headers, &columns);
      }
      for result in rdr.records() {
        if has_max && line_count >= max_line_usize {
          break;
        }
        if let Some(row) = csv_row_result_to_values(result, Arc::new(&opts.rows)) {
          rows.push(to_index_map(&row, &headers));
          line_count += 1;
        }
      }
      let total = line_count + rdr.records().count() + 1;
      let info = WorkbookInfo::simple(path_data);
      Ok(ResultSet::new(info, &headers, DataSet::WithRows(total, rows), None))
    } else {
      Err(From::from("Cannot read the CSV file"))
    }
} */

pub async fn read_csv_core<'a>(path_data: &PathData<'a>, opts: &OptionSet, save_opt:  Option<Box<dyn Fn(IndexMap<String, Value>) -> Result<(), Error> + Send + Sync>>, out_ref: Option<&str>)  -> Result<ResultSet, Error> 
  {
  let separator = match path_data.mode() {
    Extension::Tsv => b't',
    _ => b',',
  };
  if let Ok(mut rdr)= ReaderBuilder::new().delimiter(separator).from_path(path_data.path()) {
    let capture_header = opts.omit_header == false;
    let mut rows: Vec<IndexMap<String, Value>> = vec![];
    let mut line_count = 0;
    let has_max = opts.max.is_some();
    
    let max_line_usize = opts.max_rows();
    let mut headers: Vec<String> = vec![];
    // let mut has_headers = false;
    let capture_rows = opts.capture_rows();
    if capture_header {
      if let Ok(hdrs) = rdr.headers() {
          headers = hdrs.into_iter().map(|s| s.to_owned()).collect();
          // has_headers = true;
      }
      let columns = opts.rows.columns.clone();
      headers = build_header_keys(&headers, &columns);
    }
    
    let mut total = 0;;
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
      if let Ok(mut count_rdr) =  ReaderBuilder::new().from_path(&path_data.path()) {
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
    let ds = DataSet::from_count_and_rows(total, rows, opts.read_mode());
    Ok(ResultSet::new(info, &headers, ds, out_ref))
  } else {
    Err(From::from("Cannot read the CSV file"))
  }
}

fn csv_row_result_to_values(result: Result<StringRecord, csv::Error>, opts: Arc<&RowOptionSet>) -> Option<Vec<Value>> {
  if let Ok(record) = result {
    let mut row: Vec<Value> = vec![];
    let mut ci: usize = 0;
    for cell in record.into_iter() {
      let new_cell = csv_cell_to_json_value(cell, opts.clone(), ci);
      row.push(new_cell);
      ci += 1;
    }
    return  Some(row)
  }
  None
}

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
      c.euro_number_format
    } else {
      opts.euro_number_format
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
        new_cell = Value::String(cell.to_string());
    }
    new_cell
}




