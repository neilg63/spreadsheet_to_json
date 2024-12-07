mod options;
mod headers;

use std::str::FromStr;

use clap::Parser;
use csv::ReaderBuilder;
use options::Args;
use simple_string_patterns::*;
use indexmap::IndexMap;
use serde_json::json;
use chrono::NaiveDate;

use calamine::{open_workbook_auto, Data, Error,Reader};

use options::*;
use headers::*;


#[derive(Debug, Clone)]
pub struct DataSet {
    pub sheet_index: usize,
    pub sheet_key: String,
    pub sheet_refs: Vec<String>,
    pub headers: Vec<String>,
    pub data: Vec<IndexMap<String, serde_json::Value>>
}

impl DataSet {
    pub fn new(headers: &[String], data: &[IndexMap<String, serde_json::Value>], sheet_key: &str, sheet_index: usize, sheet_refs: &[String]) -> Self {
        DataSet {
            sheet_index: sheet_index,
            sheet_key: sheet_key.to_owned(),
            sheet_refs: sheet_refs.to_vec(),
            headers: headers.to_vec(),
            data: data.to_vec()
        }
    }

    pub fn to_json(&self) -> String {
        json!({
            "sheet_index": self.sheet_index,
            "sheet_key": self.sheet_key,
            "sheet_refs": self.sheet_refs,
            "headers": self.headers,
            "data": self.data
        }).to_string()
    }
}

pub fn to_dictionary(row: &[serde_json::Value], headers: &[String]) -> IndexMap<String, serde_json::Value> {
    let mut hm: IndexMap<String, serde_json::Value> = IndexMap::new();
    let mut sub_index = 0;
    for hk in headers {
        if let Some(cell) = row.get(sub_index) {
            hm.insert(hk.to_owned(), cell.to_owned());
        } 
        sub_index += 1;
    }
    hm
}

pub fn read_workbook(path: &str, sheet_opt: Option<String>, ref_sheet_index: usize) -> Result<DataSet, Error> {
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
                headers = build_header_keys(&first_row);
            }
            
            let sheet_data: Vec<Vec<serde_json::Value>> = range.rows()
            .map(|row| {
                row.iter().map(|cell| match cell {
                    Data::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap()),
                    Data::DateTime(d) => {
                        let ndt = d.as_datetime();
                        let dt_ref = if let Some(dt) = ndt {
                            dt.format("%Y-%m-%dT%H:%M:%S").to_string()
                        } else {
                            "".to_string()
                        };
                        serde_json::Value::String(dt_ref)
                    },
                    Data::Bool(b) => serde_json::Value::Bool(*b),
                    // For other types, convert to string since JSON can't directly represent them as unquoted values
                    _ => serde_json::Value::String(cell.to_string()),
                }).collect()
            })
            .collect();
            let mut sheet_map: Vec<IndexMap<String, serde_json::Value>> = vec![];
            let mut row_index = 0;
            for row in sheet_data {
                if row_index > 0 {
                    sheet_map.push(to_dictionary(&row, &headers))
                }
                row_index += 1;
            }
            Ok(DataSet::new(&headers, &sheet_map, &first_sheet_name, sheet_index, &sheet_names))
        } else {
            Err(From::from("the workbook does not have any sheets"))
        }
    }  else {
        Err(From::from("Cannot open the workbook"))
    }
}

pub fn is_euro_number_format(txt: &str, enforce_with_single_dot: bool) -> bool {
    let chs = txt.char_indices();
    let mut dot_pos: Option<usize> = None;
    let mut num_dots = 0;
    let mut comma_pos: Option<usize> = None;
    for (index, ch) in chs {
        match ch {
            '.' => {
                if dot_pos.is_none() {
                    dot_pos = Some(index);
                }
                num_dots += 1;
            }
            ',' => if comma_pos.is_none() {
                comma_pos = Some(index);
            },
            _ => ()
        }
    }
    if let Some(d_pos) = dot_pos {
        if let Some(c_pos) = comma_pos {
            d_pos < c_pos
        } else {
            num_dots > 1 || enforce_with_single_dot
        }
    } else {
        false
    }
}

pub fn read_csv(path: &str, max_lines: Option<u32>, capture_header: bool, enforce_euro_number_format: bool) -> Result<DataSet, Error> {
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
                        headers = build_header_keys(&first_row);
                        
                    } else {
                        rows.push(to_dictionary(&row, &headers));
                    }
                    
                }
            }
        }
       Ok(DataSet::new(&headers, &rows, "none", 0, &[]))
    } else {
        Err(From::from("Cannot read the CSV file"))
    }
}



pub fn render_spreadsheet(opts: &OptionSet) -> Result<(), Error> {
    // let base_path = dotenv::var("DEfAULT_SOURCE_DIR").unwrap_or(".".to_string());
    if let Some(file_path) = opts.path.clone() {
            // let file_name = "labour-vote-share.csv";
        let enforce_euro_number_format = false;
        //let path = format!("{}/{}", base_path, file_name);
        let sheet_key = opts.sheet.clone();
        let sheet_index = opts.index as usize;
        let extension = file_path.to_end(".").to_lowercase();
        // println!("path: {}, {}", path, extension);

        let data_set_result = match extension.as_str() {
            "xlsx" | "xls" | "ods" => read_workbook(&file_path, sheet_key, sheet_index),
            "csv" => read_csv(&file_path, None, true, enforce_euro_number_format),
            _ => Err(From::from("Unsupported format"))
        };
        
        match data_set_result {
            Ok(data_set) => {
                println!("{}", data_set.to_json());
                Ok(())
            },
            Err(msg) => Err(msg)
            
        }
    } else {
        Err(From::from("No file path specified"))
    }
    
}


fn main() {
    let args = Args::parse();
    let opts = OptionSet::from_args(&args);
    
   let result = render_spreadsheet(&opts);
   if result.is_err() {
    println!("{:?}", result.err());
   }

}
