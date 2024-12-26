
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

pub fn to_padded_col_suffix(prefix: &str, index: usize, num_cols: usize) -> String {
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
  format!("{}{:0width$}", prefix, num, width = width)
}

pub fn to_c01_col_key(index: usize, num_cols: usize) -> String {
  to_padded_col_suffix("c", index, num_cols)
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
            if let Some(k_str) = &col.key {
              let h_key = if headers.contains(&k_str.to_string()) {
                to_padded_col_suffix(k_str, h_index, num_cols)
              } else {
                k_str.to_string()
              };
              headers.push(h_key);
              has_override = true;
            }
        }
        if !has_override {
            if keep_headers && sn.len() > 0 {
                let sn_key = if headers.contains(&sn) {
                    to_padded_col_suffix(&sn, h_index, num_cols)
                } else {
                    sn
                };
                headers.push(sn_key);
            } else {
                headers.push(to_head_key(h_index, field_mode, num_cols));
            }
        }
        h_index += 1;
    }
    headers
}

/// Assign keys with A1+ notation
pub fn build_a1_headers(first_row: &[String]) -> Vec<String> {
    build_header_keys(first_row, &[], &FieldNameMode::A1)
}

/// Assign keys as c + zero-padded number
pub fn build_c01_headers(first_row: &[String]) -> Vec<String> {
    build_header_keys(first_row, &[], &FieldNameMode::NumPadded)
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
    use simple_string_patterns::ToStrings;

    use crate::Format;

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

    #[test]
    fn test_first_row() {
        // header labels as captured from the top row
        let first_row = ["Viscosity", "Rating", "", ""].to_strings();
        let cols = vec![
            Column::from_key_ref_with_format(None, Format::Float, None, false, false),
            Column::from_key_ref_with_format(Some("points"), Format::Decimal(3), None, false, false),
            Column::from_key_ref_with_format(Some("adjusted"), Format::Float, None, false, false),
        ];
        let headers = build_header_keys(&first_row, &cols, &FieldNameMode::AutoA1);
        // should be lower-cased as `viscosity`
        assert_eq!(headers.get(0).unwrap(), "viscosity");
        // should be overridden as `points`
        assert_eq!(headers.get(1).unwrap(), "points");
        // should be labelled `adjusted`
        assert_eq!(headers.get(2).unwrap(), "adjusted");
        // fourth column  with empty heading should be assigned an A1-style key of `d`
        assert_eq!(headers.get(3).unwrap(), "d");
    }

    #[test]
    fn test_headers_a1_override() {
        // header labels as captured from the top row
        let first_row = ["Viscosity", "Rating", "Weighted", "Class"].to_strings();
        
        let headers = build_a1_headers(&first_row);
        // should be lower-cased as `viscosity`
        assert_eq!(headers.get(0).unwrap(), "a");
        // the column should be d.
        assert_eq!(headers.get(3).unwrap(), "d");
    }

    #[test]
    fn test_headers_c01_override() {
        // build header row with 200 sequential alphanumeric values
        let first_row: Vec<String> = (0..200).map(|x| [char::from_u32(65 + (x % 26)).unwrap_or('_').to_string(), (x * 3 * 1).to_string()].concat()).collect();
        
        let headers = build_c01_headers(&first_row);
        // should be lower-cased as `viscosity`
        assert_eq!(headers.get(0).unwrap(), "c001");
        // the column should be d.
        assert_eq!(headers.get(3).unwrap(), "c004");
    }
}