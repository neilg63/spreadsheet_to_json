
use heck::ToSnakeCase;
use indexmap::IndexMap;
use serde_json::Value;

use crate::{Column, FieldNameMode};

pub fn to_letter(index: u32) -> char {
    char::from_u32(97 + index).unwrap_or(' ') // Use 97 for 'a'
}

pub fn to_a1_col_key(index: usize) -> String {
    let mut result = String::new();
    let mut n = index as i32; // Work with i32 to handle potential negative values

    while n >= 0 {
        let remainder = (n % 26) as u8;
        result.push((b'a' + remainder) as char);
        n = (n / 26) - 1;
    }
    result.chars().rev().collect()
}

pub fn to_c01_col_key(index: usize, num_cols: usize) -> String {
    let width = if num_cols < 100 {
        2
    } else if num_cols < 1000 {
        3
    } else if num_cols < 10000 {
        4
    } else {
        5
    };
    let num = index + 1;
    format!("c{:0width$}", num, width = width)
}

pub fn to_head_key(index: usize, field_mode: &FieldNameMode, num_cols: usize) -> String {
    if field_mode.use_c01() {
        to_c01_col_key(index, num_cols)
    } else {
        to_a1_col_key(index)
    }
}

pub fn to_head_key_default(index: usize) -> String {
    to_c01_col_key(index, 1000)
}

/// Build header keys from the first row of a CSV file or headers captured from a spreadsheet
pub fn build_header_keys(first_row: &[String], columns: &[Column], field_mode: &FieldNameMode) -> Vec<String> {
let mut h_index = 0;
    let mut headers: Vec<String> = vec![];
    let num_cols = first_row.len();
    let keep_headers = field_mode.keep_headers();
    for h_row in first_row.to_owned() {
        let sn = h_row.to_snake_case();
        let mut has_override = false;
        if let Some(col) = columns.get(h_index) {
            // only apply override if key is not empty
            if col.key.len() > 0 {
                headers.push(col.key.to_string());
                has_override = true;
            }
        } 
        if !has_override {
            if keep_headers && sn.len() > 0 {
                headers.push(sn);
            } else {
                headers.push(to_head_key(h_index, field_mode, num_cols));
            }
        }
        h_index += 1;
    }
    headers
}


/// check if the row is not a header row. Always return true if row_index is greater than 0
pub(crate) fn is_not_header_row(row_map: &IndexMap<String, Value>, row_index: usize, headers: &[String]) -> bool {
  if row_index > 0 {
      return true;
  }
  let mut is_header = true;
  for (_key, value) in row_map.iter() {
    let ref_key = value.to_string().to_snake_case();
    if !headers.contains(&ref_key) {
      is_header = false;
      break;
    }
  }
  is_header
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_letter() {

        assert_eq!(to_letter(5), 'f');

        assert_eq!(to_letter(25), 'z');
    }

    #[test]
    fn test_cell_letters_1() {

        assert_eq!(to_a1_col_key(26), "aa");
    }

    #[test]
    fn test_cell_letters_2() {

        assert_eq!(to_a1_col_key(701), "zz");
    }

    #[test]
    fn test_cell_letters_3() {

        assert_eq!(to_a1_col_key(702), "aaa");
    }

    #[test]
    fn test_cell_letters_4() {

        assert_eq!(to_c01_col_key(8, 60), "c09");
    }

    #[test]
    fn test_cell_letters_5() {
        assert_eq!(to_c01_col_key(20, 750), "c021");
    }

    #[test]
    fn test_cell_letters_6() {
        assert_eq!(to_c01_col_key(20, 2000), "c0021");
    }
}