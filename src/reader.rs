use std::str::FromStr;

use csv::ReaderBuilder;
use serde_json::{Number, Value};
use simple_string_patterns::*;
use indexmap::IndexMap;
use std::path::Path;

use calamine::{open_workbook_auto, Data, Error,Reader};

use crate::headers::*;
use crate::data_set::*;
use crate::Column;
use crate::Format;
use crate::OptionSet;
use crate::euro_number_format::is_euro_number_format;

pub fn render_spreadsheet(opts: &OptionSet) -> Result<DataSet, Error> {
    
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


pub fn read_workbook(path: &Path, extension: &str, opts: &OptionSet) -> Result<DataSet, Error> {
    if path.exists() == false {
        return Err(From::from("the file $filepath does not exist"));
    }
    if let Ok(mut workbook) = open_workbook_auto(path) {
      let columns = opts.columns.clone();
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
            if let Some(first_row) = range.headers() {
                headers = build_header_keys(&first_row, &columns);
            }
            let sheet_data: Vec<Vec<Value>> = range.rows()
            .map(|row| {
              let mut cells: Vec<Value> = vec![];
              let mut c_index = 0;
              let format = if let Some(col) = columns.get(c_index) {
                col.format.clone()   
              } else {
                Format::Auto
              };
              for cell in row {
                let new_cell = match cell {
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
                            _ => "%Y-%m-%dT%H:%M:%S.000Z"
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
                  _ => Value::String(cell.to_string()),
                };
                
                cells.push(new_cell);
                c_index += 1;
              }
              cells
            })
            .collect();
            let mut sheet_map: Vec<IndexMap<String, Value>> = vec![];
            let mut row_index = 0;
            for row in sheet_data {
                if row_index > 0 || opts.omit_header {
                    sheet_map.push(to_dictionary(&row, &headers))
                }
                row_index += 1;
            }
            Ok(DataSet::new(&extract_file_name(path), extension, &headers, &sheet_map, &first_sheet_name, sheet_index, &sheet_names))
        } else {
            Err(From::from("the workbook does not have any sheets"))
        }
    }  else {
        Err(From::from("Cannot open the workbook"))
    }
}

pub fn read_csv(path: &Path, extension: &str, opts: &OptionSet) -> Result<DataSet, Error> {
    if let Ok(mut rdr)= ReaderBuilder::new().from_path(path) {
      let columns = opts.columns.clone();
      let capture_header = opts.omit_header == false;
      let mut rows: Vec<IndexMap<String, Value>> = vec![];
      let mut line_count = 0;
      let has_max = opts.max.is_some();
      let max_line_usize = if has_max {
          opts.max.unwrap_or(1000) as usize
      } else {
          1000
      };
      let mut headers: Vec<String> = vec![];
      for result in rdr.records() {
          if let Ok(record) = result {
              let mut row: Vec<Value> = vec![];
              for cell in record.into_iter() {
                  if has_max && line_count >= max_line_usize {
                      break;
                  }
                  let has_number = cell.to_first_number::<f64>().is_some();
                  let num_cell = if has_number {
                      let euro_num_mode = is_euro_number_format(cell, opts.euro_number_format);
                      if euro_num_mode {
                          cell.replace(",", ".").replace(",", ".")
                      } else {
                          cell.replace(",", "")
                      }
                  } else {
                      cell.to_owned()
                  };
                  if num_cell.is_numeric() {
                      if let Ok(float_val) = serde_json::Number::from_str(&num_cell) {
                          row.push(Value::Number(float_val));
                      }
                  } else {
                      row.push(Value::String(cell.to_string()));
                  }
                  line_count += 1;
                  if capture_header && line_count < 2 {
                      let first_row = row.clone().into_iter().map(|v| v.to_string()).collect::<Vec<String>>();
                      headers = build_header_keys(&first_row, &columns);
                      
                  } else {
                      rows.push(to_dictionary(&row, &headers));
                  }
                  
              }
          }
        }
       Ok(DataSet::new(&extract_file_name(path), extension, &headers, &rows, "none", 0, &[]))
    } else {
        Err(From::from("Cannot read the CSV file"))
    }
}

pub fn extract_file_name(path: &Path) -> String {
    if let Some(file_ref) = path.file_name() {
        file_ref.to_string_lossy().to_string()
    } else {
        "".to_owned()
    }
}
