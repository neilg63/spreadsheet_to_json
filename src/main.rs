use std::str::FromStr;

use csv::ReaderBuilder;
use simple_string_patterns::*;
use heck::ToSnakeCase;
use indexmap::IndexMap;
use serde_json::json;
use chrono::NaiveDate;

use calamine::{open_workbook_auto, Data, Error,Reader};

mod options;


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

pub fn to_radix_groups_with_offset_option(n: u32, radix: u32, index_offset: bool) -> Vec<u32> {
    let mut result = Vec::new();
    let mut num = n;
    let mut not_first = false;
    while num > 0 {
        let offset = if not_first && index_offset {
            1
        } else {
            0
        };
        result.push(num % radix - offset);
        num /= radix;
        not_first = true;
    }

    result.reverse(); // Because we've pushed the least significant digit first
    result
}

pub fn to_radix_groups(n: u32, radix: u32) -> Vec<u32> {
    to_radix_groups_with_offset_option(n, radix, false)
}


pub fn to_radix_groups_offset(n: u32, radix: u32) -> Vec<u32> {
    to_radix_groups_with_offset_option(n, radix, true)
}

pub fn to_letter(index: u32) -> char {
    char::from_u32(97 + index).unwrap_or(' ')
}

pub fn to_head_key(index: usize) -> String {
    if index < 1 {
        'a'.to_string()
    } else {
        let groups = to_radix_groups_offset(index as u32, 26);
        groups.into_iter().map(|ci| to_letter(ci)).collect::<String>() 
    }
}

pub fn build_header_keys(first_row: &[String]) -> Vec<String> {
let mut h_index = 0;
    let num_cells = first_row.len();
    let mut headers: Vec<String> = vec![];
    let num_pop_header_cells = first_row.to_owned().into_iter().filter(|sn| sn.to_snake_case().len() > 0).collect::<Vec<String>>().len();
    let add_custom_headers = num_pop_header_cells >= num_cells;
    for h_row in first_row.to_owned() {
        let sn = h_row.to_snake_case();
        if add_custom_headers {
            headers.push(sn);
        } else {
            headers.push(to_head_key(h_index));
        }
        h_index += 1;
    }
    headers
}

pub fn read_workbook(path: &str, sheet_key: &str, ref_sheet_index: usize) -> Result<DataSet, Error> {
    if let Ok(mut workbook) = open_workbook_auto(path) {
        let mut sheet_index = ref_sheet_index;
        let sheet_names = workbook.worksheets().into_iter().map(|ws| ws.0).collect::<Vec<String>>();
        let key_string = sheet_key.strip_spaces().to_lowercase();
        if let Some(s_index) = sheet_names.clone().into_iter().position(|sn| sn.strip_spaces().to_lowercase() == key_string) {
            sheet_index = s_index;
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



pub fn render_spreadsheet() -> Result<(), Error> {
    let base_path = dotenv::var("DEfAULT_SOURCE_DIR").unwrap_or(".".to_string());
    let file_name = "project-management.xlsx";
    // let file_name = "labour-vote-share.csv";
    let enforce_euro_number_format = false;
    let path = format!("{}/{}", base_path, file_name);
    let sheet_key = "Sheet1";
    let sheet_index = 0;
    let extension = path.to_end(".").to_lowercase();
    // println!("path: {}, {}", path, extension);

    let data_set_result = match extension.as_str() {
        "xlsx" | "xls" | "ods" => read_workbook(&path, &sheet_key, sheet_index),
        "csv" => read_csv(&path, None, true, enforce_euro_number_format),
        _ => Err(From::from("Unsupported format"))
    };
    
    match data_set_result {
        Ok(data_set) => {
            println!("{}", data_set.to_json());
            Ok(())
        },
        Err(msg) => Err(msg)
        
    }
    
}


fn main() {

   let result = render_spreadsheet();
   if result.is_err() {
    println!("{:?}", result.err());
   }

}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_letter() {

        assert_eq!(to_letter(5), 'f');

        assert_eq!(to_letter(25), 'z');
    }

    #[test]
    fn test_radix_groups() {

        assert_eq!(to_radix_groups(27, 26), vec![1, 1]);


        assert_eq!(to_radix_groups(24, 26), vec![24]);

        assert_eq!(to_radix_groups(56, 26), vec![2, 4]);
    }

    #[test]
    fn test_radix_groups_offset() {

        assert_eq!(to_radix_groups_offset(27, 26), vec![0, 1]);

        assert_eq!(to_radix_groups_offset(26, 26), vec![0, 0]);


        assert_eq!(to_radix_groups_offset(24, 26), vec![24]);

        assert_eq!(to_radix_groups_offset(56, 26), vec![1, 4]);
    }

    #[test]
    fn test_cell_letters() {

        assert_eq!(to_head_key(26), "aa");
    }
}
