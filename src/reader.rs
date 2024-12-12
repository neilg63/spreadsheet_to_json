use std::str::FromStr;

use csv::ReaderBuilder;
use csv::StringRecord;
use heck::ToSnakeCase;
use serde_json::{Number, Value};
use simple_string_patterns::*;
use indexmap::IndexMap;
use std::path::Path;

use calamine::{open_workbook_auto, Data, Error,Reader};

use crate::headers::*;
use crate::data_set::*;
use crate::is_truthy::is_truthy_core;
use crate::Format;
use crate::OptionSet;
use crate::euro_number_format::is_euro_number_format;

pub fn render_spreadsheet(opts: &OptionSet) -> Result<ResultSet, Error> {
    
    if let Some(filepath) = opts.path.clone() {
        let path = Path::new(&filepath);
        if !path.exists() {
            let canonical_path = path.canonicalize()?;
            let fpath = canonical_path.to_str().unwrap_or("");
            return Err(From::from("The file $fpath is not available"));
        }
        let extension = filepath.to_end(".").to_lowercase();
        match extension.as_str() {
            "xlsx" | "xls" | "ods" => read_workbook(path, &extension, opts),
            "csv" => read_csv(path, &extension, opts),
            _ => Err(From::from("Unsupported format"))
        }
    } else {
        Err(From::from("No file path specified"))
    }
}


pub fn read_workbook(path: &Path, extension: &str, opts: &OptionSet) -> Result<ResultSet, Error> {
    if path.exists() == false {
        return Err(From::from("the file $filepath does not exist"));
    }
    if let Ok(mut workbook) = open_workbook_auto(path) {
      let columns = opts.columns.clone();
      let max_rows = opts.max_rows();
        let mut sheet_index = opts.index as usize;
        let sheet_names = workbook.worksheets().into_iter().map(|ws| ws.0).collect::<Vec<String>>();
        if let Some(sheet_key) = opts.sheet.clone() {
            let key_string = sheet_key.strip_spaces().to_lowercase();
            if let Some(s_index) = sheet_names.clone().into_iter().position(|sn| sn.strip_spaces().to_lowercase() == key_string) {
                sheet_index = s_index;
            }
        }
        if let Some(first_sheet_name) = sheet_names.get(sheet_index) {
            let range = workbook.worksheet_range(first_sheet_name)?;
            let mut headers: Vec<String> = vec![];
            let mut has_headers = false;
            let capture_headers = !opts.omit_header;
            if let Some(first_row) = range.headers() {
                headers = build_header_keys(&first_row, &columns);
                has_headers = true;
            }
            let source_rows  = range.rows();
            let mut sheet_map: Vec<IndexMap<String, Value>> = vec![];
            let mut row_index = 0;
            let header_row_index = opts.header_row_index();
            let match_header_row_below = capture_headers && header_row_index > 0;
            for row in source_rows {
              if row_index >= max_rows {
                break;
              }
              if match_header_row_below && (row_index + 1) == header_row_index {
                let h_row = row.into_iter().map(|c| c.to_string().to_snake_case()).collect::<Vec<String>>();
                headers = build_header_keys(&h_row, &columns);
              } else if has_headers || !capture_headers {
                let cells = workbook_row_to_values(row, opts);
                sheet_map.push(to_dictionary(&cells, &headers));
              }
              row_index += 1;
            }
            let info = WorkbookInfo::new(&extract_file_name(&path), extension, &first_sheet_name, sheet_index, &sheet_names);
            Ok(ResultSet::new(info, &headers, DataSet::Rows(sheet_map)))
        } else {
            Err(From::from("the workbook does not have any sheets"))
        }
    }  else {
        Err(From::from("Cannot open the workbook"))
    }
}

fn workbook_row_to_values(row: &[Data], opts: &OptionSet) -> Vec<Value> {
  let mut c_index = 0;
  let mut cells: Vec<Value> = vec![];
  for cell in row {
    let value = workbook_cell_to_value(cell, opts, c_index);
    cells.push(value);
    c_index += 1;
  }
  cells
}

fn workbook_cell_to_value(cell:&Data, opts: &OptionSet, c_index: usize) -> Value {
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

pub fn read_csv(path: &Path, extension: &str, opts: &OptionSet) -> Result<ResultSet, Error> {
    if let Ok(mut rdr)= ReaderBuilder::new().from_path(path) {
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
        let columns = opts.columns.clone();
        headers = build_header_keys(&headers, &columns);
      }
      for result in rdr.records() {
        if has_max && line_count >= max_line_usize {
          break;
        }
        if let Some(row) = csv_row_result_to_values(result, opts) {
          rows.push(to_dictionary(&row, &headers));
          line_count += 1;
        }
      }
      let info = WorkbookInfo::simple(&extract_file_name(&path), extension);
      Ok(ResultSet::new(info, &headers, DataSet::Rows(rows)))
    } else {
      Err(From::from("Cannot read the CSV file"))
    }
}

fn csv_row_result_to_values(result: Result<StringRecord, csv::Error>, opts: &OptionSet) -> Option<Vec<Value>> {
  if let Ok(record) = result {
    let mut row: Vec<Value> = vec![];
    let mut ci: usize = 0;
    for cell in record.into_iter() {
      let new_cell = csv_cell_to_json_value(cell, opts, ci);
      row.push(new_cell);
      ci += 1;
    }
    return  Some(row)
  }
  None
}

fn csv_cell_to_json_value(cell: &str, opts: &OptionSet, index: usize) -> Value {
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



pub fn extract_file_name(path: &Path) -> String {
    if let Some(file_ref) = path.file_name() {
        file_ref.to_string_lossy().to_string()
    } else {
        "".to_owned()
    }
}
