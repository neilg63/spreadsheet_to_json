use std::str::FromStr;

use csv::ReaderBuilder;
use simple_string_patterns::*;
use indexmap::IndexMap;
use std::path::Path;

use calamine::{open_workbook_auto, Data, Error,Reader};

use crate::headers::*;
use crate::data_set::*;
use crate::Column;
use crate::Format;
use crate::OptionSet;

pub fn render_spreadsheet(opts: &OptionSet) -> Result<DataSet, Error> {
    
    if let Some(filepath) = opts.path.clone() {
        let path = Path::new(&filepath);
        if !path.exists() {
            let canonical_path = path.canonicalize()?;
            let fpath = canonical_path.to_str().unwrap_or("");
            return Err(From::from("The file $fpath is not available"));
        }
        let enforce_euro_number_format = false;
        let sheet_key = opts.sheet.clone();
        let sheet_index = opts.index as usize;
        let extension = filepath.to_end(".").to_lowercase();
        let omit_header = opts.omit_header;
        let columns = opts.columns.clone();
        match extension.as_str() {
            "xlsx" | "xls" | "ods" => read_workbook(path, &extension, sheet_key, sheet_index, omit_header, &columns),
            "csv" => read_csv(path, &extension, None, !omit_header, enforce_euro_number_format, &columns),
            _ => Err(From::from("Unsupported format"))
        }
    } else {
        Err(From::from("No file path specified"))
    }
}


pub fn read_workbook(path: &Path, extension: &str, sheet_opt: Option<String>, ref_sheet_index: usize, omit_header: bool, columns: &[Column]) -> Result<DataSet, Error> {
    if path.exists() == false {
        return Err(From::from("the file $filepath does not exist"));
    }
    if let Ok(mut workbook) = open_workbook_auto(path) {
        let mut sheet_index = ref_sheet_index;
        let sheet_names = workbook.worksheets().into_iter().map(|ws| ws.0).collect::<Vec<String>>();
        if let Some(sheet_key) = sheet_opt {
            let key_string = sheet_key.strip_spaces().to_lowercase();
            if let Some(s_index) = sheet_names.clone().into_iter().position(|sn| sn.strip_spaces().to_lowercase() == key_string) {
                sheet_index = s_index;
            }
        }
        if let Some(first_sheet_name) = sheet_names.get(sheet_index) {
            let range = workbook.worksheet_range(first_sheet_name)?;
            let mut headers: Vec<String> = vec![];
            if let Some(first_row) = range.headers() {
                headers = build_header_keys(&first_row, columns);
            }
            let sheet_data: Vec<Vec<serde_json::Value>> = range.rows()
            .map(|row| {
              let mut cells: Vec<serde_json::Value> = vec![];
              let mut c_index = 0;
              let format = if let Some(col) = columns.get(c_index) {
                col.format.clone()   
              } else {
                Format::Auto
              };
              for cell in row {
                let new_cell = match cell {
                  Data::Int(i) => serde_json::Value::Number(serde_json::Number::from_i128(*i as i128).unwrap()),
                  Data::Float(f) => {
                    match format {
                      Format::Integer => serde_json::Value::Number(serde_json::Number::from_i128(*f as i128).unwrap()),
                      _ => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap())
                    }
                  },
                  Data::DateTimeIso(d) => serde_json::Value::String(d.to_owned()),
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
                      serde_json::Value::String(dt_ref)
                  },
                  Data::Bool(b) => serde_json::Value::Bool(*b),
                  // For other types, convert to string since JSON can't directly represent them as unquoted values
                  _ => serde_json::Value::String(cell.to_string()),
                };
                
                cells.push(new_cell);
                c_index += 1;
              }
              cells
            })
            .collect();
            let mut sheet_map: Vec<IndexMap<String, serde_json::Value>> = vec![];
            let mut row_index = 0;
            for row in sheet_data {
                if row_index > 0 || omit_header {
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

pub fn read_csv(path: &Path, extension: &str, max_lines: Option<u32>, capture_header: bool, enforce_euro_number_format: bool, columns: &[Column]) -> Result<DataSet, Error> {
    if let Ok(mut rdr)= ReaderBuilder::new().from_path(path) {
        let mut rows: Vec<IndexMap<String, serde_json::Value>> = vec![];
        let mut line_count = 0;
        let has_max = max_lines.is_some();
        let max_line_usize = if has_max {
            max_lines.unwrap_or(1000) as usize
        } else {
            1000
        };
        let mut headers: Vec<String> = vec![];
        for result in rdr.records() {
            if let Ok(record) = result {
                let mut row: Vec<serde_json::Value> = vec![];
                for cell in record.into_iter() {
                    if has_max && line_count >= max_line_usize {
                        break;
                    }
                    let has_number = cell.to_first_number::<f64>().is_some();
                    let num_cell = if has_number {
                        let euro_num_mode = is_euro_number_format(cell, enforce_euro_number_format);
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
                            row.push(serde_json::Value::Number(float_val));
                        }
                    } else {
                        row.push(serde_json::Value::String(cell.to_string()));
                    }
                    line_count += 1;
                    if capture_header && line_count < 2 {
                        let first_row = row.clone().into_iter().map(|v| v.to_string()).collect::<Vec<String>>();
                        headers = build_header_keys(&first_row, columns);
                        
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
        file_ref.to_str().unwrap_or("")
    } else {
        ""
    }.to_owned()
}

/// Detect if a numeric string uses the European format with , as the decimal separator and dots as thousand separators
/// If only one comma is present, *enforce_euro_mode* will treat the comma as a decimal separator.
/// Otherwise it will be assumed to be a thousand separator
pub fn is_euro_number_format(txt: &str, enforce_euro_mode: bool) -> bool {
    let chs = txt.char_indices();
    let mut num_indices = 0;
    let mut dot_pos: Option<usize> = None;
    let mut num_dots = 0;
    let mut num_commas = 0;
    let mut comma_pos: Option<usize> = None;
    for (index, ch) in chs {
        match ch {
            '.' => {
                if dot_pos.is_none() {
                    dot_pos = Some(index);
                }
                num_dots += 1;
            }
            ',' => {
                if comma_pos.is_none() {
                    comma_pos = Some(index);
                }
                num_commas += 1;
            },
            _ => ()
        }
        num_indices += 1; // count indices here to avoid cloning above
    }
    if let Some(d_pos) = dot_pos {
        if let Some(c_pos) = comma_pos {
            d_pos < c_pos
        } else {
            // if it only has one dot only interpreet as decimal separator if enforce_euro_mode is true
            num_dots > 1 || enforce_euro_mode
        }
    } else {
        // no dots
        if let Some(c_pos) = comma_pos {
            if num_commas > 1 {
                false
            } else {
                // with only one comma, assume it's a decimal separator
                // if it is not exactly 4 positions from the right or if enforce
                num_indices - c_pos != 4 || enforce_euro_mode
            }
        } else {
            false
        }

    }
}



#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_lnumber_format_1() {
        let sample = "1.256";
        assert_eq!(is_euro_number_format(sample, false), false);
    }

    #[test]
    fn test_lnumber_format_2() {
        let sample = "1,256";
        assert_eq!(is_euro_number_format(sample, false), false);
    }

    #[test]
    fn test_lnumber_format_3() {

        let sample = "12,56";
        
        assert_eq!(is_euro_number_format(sample, false), true);
    }

    #[test]
    fn test_lnumber_format_4() {

        let sample = "1,256.67";
        
        assert_eq!(is_euro_number_format(sample, false), false);
    }

    #[test]
    fn test_lnumber_format_5() {

        let sample = "1.256,67";
        
        assert_eq!(is_euro_number_format(sample, false), true);
    }
}