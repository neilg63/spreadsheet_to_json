use heck::ToSnakeCase;

use crate::{Column, FieldNameMode};

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

pub fn to_padded_col_key(prefix: &str, index: usize, num_cols: usize) -> String {
    build_padded_col_key(prefix, false, index, num_cols)
}

pub fn to_padded_col_suffix(prefix: &str, index: usize, num_cols: usize) -> String {
    build_padded_col_key(prefix, true, index, num_cols)
}

fn build_padded_col_key(prefix: &str, underscore: bool, index: usize, num_cols: usize) -> String {
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
    let separator = if underscore { "_" } else { "" };
    format!("{}{}{:0width$}", prefix, separator, num, width = width)
}

pub fn to_c01_col_key(index: usize, num_cols: usize) -> String {
    to_padded_col_key("c", index, num_cols)
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
pub fn build_header_keys(
    first_row: &[String],
    columns: &[Column],
    field_mode: &FieldNameMode,
) -> Vec<String> {
    let mut headers: Vec<String> = vec![];
    let num_cols = first_row.len();
    let keep_headers = field_mode.keep_headers();
    for (h_index, h_row) in first_row.iter().enumerate() {
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
            if keep_headers && !sn.is_empty() {
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
    }
    headers
}

/// The natural (un-overridden) key for each column, exactly as build_header_keys would
/// derive it with no column overrides at all. Used as the matching target for column
/// overrides that reference a column by its source_key rather than by position.
pub fn natural_column_keys(first_row: &[String], field_mode: &FieldNameMode) -> Vec<String> {
    build_header_keys(first_row, &[], field_mode)
}

/// Resolve a (possibly unordered, possibly sparse) list of column overrides against a
/// sheet's natural header keys, producing one Column per natural column, aligned by index.
///
/// Overrides with a `source_key` are matched by name against the natural keys wherever
/// that column actually is, regardless of the override's position in `columns` — this is
/// what lets a caller override just one field out of many (e.g. `weight_kg -> weight`)
/// without needing to enumerate every column ahead of it. Overrides with no `source_key`
/// keep applying positionally instead, exactly as before, for backward compatibility with
/// direct library use.
pub fn resolve_columns(columns: &[Column], natural_keys: &[String]) -> Vec<Column> {
    let mut resolved: Vec<Column> = natural_keys.iter().map(|_| Column::new(None)).collect();
    for (i, col) in columns.iter().enumerate() {
        if col.source_key.is_none() {
            if let Some(slot) = resolved.get_mut(i) {
                *slot = col.clone();
            }
        }
    }
    for col in columns {
        if let Some(src) = &col.source_key {
            let target = src.to_snake_case();
            if let Some(idx) = natural_keys.iter().position(|k| k.to_snake_case() == target) {
                resolved[idx] = col.clone();
            }
        }
    }
    resolved
}

/// Assign keys with A1+ notation
pub fn build_a1_headers(first_row: &[String]) -> Vec<String> {
    build_header_keys(first_row, &[], &FieldNameMode::A1)
}

/// Assign keys as c + zero-padded number
pub fn build_c01_headers(first_row: &[String]) -> Vec<String> {
    build_header_keys(first_row, &[], &FieldNameMode::NumPadded)
}

/// Check if the row is not a header row. Always returns true if row_index is greater than 0.
///
/// Compares the row's *raw*, un-coerced cell text against the raw header text -- not the
/// row's already-formatted values. Comparing post-format values used to break this check
/// whenever a column had a non-Auto Format: coercing the header row's own text through that
/// format (e.g. a decimal parse, or a date parse) commonly turns it into `null` or some other
/// value that no longer equals the header text, so the header row would be misclassified as
/// real data and leak into the output.
pub(crate) fn is_not_header_row(
    raw_values: &[String],
    row_index: usize,
    headers: &[String],
) -> bool {
    if row_index > 0 {
        return true;
    }
    let mut num_matched: usize = 0;
    for (h_index, hk) in headers.iter().enumerate() {
        let sn = hk.to_snake_case();
        if let Some(val) = raw_values.get(h_index) {
            if val.to_snake_case() == sn || sn.is_empty() {
                num_matched += 1;
            }
        }
    }
    num_matched < headers.len()
}

#[cfg(test)]
mod tests {

    use crate::Format;

    use super::*;

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
    fn test_is_not_header_row_uses_raw_text_not_coerced_values() {
        // Regression test: comparing against a row's *coerced* Format-applied values used to
        // misclassify the header row as real data whenever a column had a non-Auto format,
        // because coercing the header row's own text (e.g. "weight_kg") through that format
        // (e.g. Format::Decimal) turned it into null/something else that no longer matched
        // the header text. Comparing raw, un-coerced cell text sidesteps that entirely.
        let headers = vec!["sku".to_string(), "weight".to_string()];
        // the header row repeated verbatim as "data" -- should be detected and excluded
        let header_row_raw = vec!["sku".to_string(), "weight".to_string()];
        assert!(!is_not_header_row(&header_row_raw, 0, &headers));

        // genuine data at row 0 (e.g. a headerless sheet) is not excluded
        let data_row_raw = vec!["SKU001".to_string(), "58.2".to_string()];
        assert!(is_not_header_row(&data_row_raw, 0, &headers));

        // row_index > 0 is always real data, regardless of content
        assert!(is_not_header_row(&header_row_raw, 1, &headers));
    }

    #[test]
    fn test_resolve_columns_matches_by_source_key_regardless_of_position() {
        // "full_name,height_cm,weight_kg" -- override only weight_kg, out of order and
        // without needing to pad the other two columns with empty entries.
        let first_row = ["full_name", "height_cm", "weight_kg"].map(|s| s.to_string());
        let natural_keys = natural_column_keys(&first_row, &FieldNameMode::AutoA1);
        assert_eq!(natural_keys, vec!["full_name", "height_cm", "weight_kg"]);

        let overrides = vec![
            Column::from_source_key_with_format("weight_kg", Some("weight"), Format::Integer, None, false, false),
        ];
        let resolved = resolve_columns(&overrides, &natural_keys);
        assert_eq!(resolved.len(), 3);
        // untouched columns keep their natural key and Format::Auto
        assert!(resolved[0].key.is_none());
        assert!(resolved[1].key.is_none());
        // the matched column picked up the override regardless of its position in `overrides`
        assert_eq!(resolved[2].key_name(), "weight");
        assert_eq!(resolved[2].format.to_string(), "integer");

        let headers = build_header_keys(&first_row, &resolved, &FieldNameMode::AutoA1);
        assert_eq!(headers, vec!["full_name", "height_cm", "weight"]);
    }

    #[test]
    fn test_resolve_columns_source_key_match_is_snake_cased() {
        // The source key is matched against the natural snake_cased header, so it
        // doesn't need to be typed in exactly the same casing/spacing as the header.
        let first_row = ["Weight (Kg)".to_string()];
        let natural_keys = natural_column_keys(&first_row, &FieldNameMode::AutoA1);
        assert_eq!(natural_keys, vec!["weight_kg"]);

        let overrides = vec![
            Column::from_source_key_with_format("Weight Kg", Some("weight"), Format::Auto, None, false, false),
        ];
        let resolved = resolve_columns(&overrides, &natural_keys);
        assert_eq!(resolved[0].key_name(), "weight");
    }

    #[test]
    fn test_resolve_columns_unmatched_source_key_is_a_no_op() {
        let first_row = ["full_name", "height_cm"].map(|s| s.to_string());
        let natural_keys = natural_column_keys(&first_row, &FieldNameMode::AutoA1);
        let overrides = vec![
            Column::from_source_key_with_format("nonexistent_field", Some("oops"), Format::Auto, None, false, false),
        ];
        let resolved = resolve_columns(&overrides, &natural_keys);
        // no column matched "nonexistent_field", so nothing changes -- silently ignored
        assert!(resolved[0].key.is_none());
        assert!(resolved[1].key.is_none());
    }

    #[test]
    fn test_resolve_columns_still_supports_positional_overrides() {
        // Columns with no source_key keep applying by position, for backward
        // compatibility with direct library use.
        let first_row = ["a", "b", "c"].map(|s| s.to_string());
        let natural_keys = natural_column_keys(&first_row, &FieldNameMode::AutoA1);
        let overrides = vec![
            Column::new(Some("first")),
            Column::new(Some("second")),
        ];
        let resolved = resolve_columns(&overrides, &natural_keys);
        assert_eq!(resolved[0].key_name(), "first");
        assert_eq!(resolved[1].key_name(), "second");
        assert!(resolved[2].key.is_none());
    }

    #[test]
    fn test_first_row() {
        // header labels as captured from the top row
        let first_row = ["Viscosity", "Rating", "", ""].map(|s| s.to_string());
        let cols = vec![
            Column::from_key_ref_with_format(None, Format::Float, None, false, false),
            Column::from_key_ref_with_format(
                Some("points"),
                Format::Decimal(3),
                None,
                false,
                false,
            ),
            Column::from_key_ref_with_format(Some("adjusted"), Format::Float, None, false, false),
        ];
        let headers = build_header_keys(&first_row, &cols, &FieldNameMode::AutoA1);
        // should be lower-cased as `viscosity`
        assert_eq!(headers.first().unwrap(), "viscosity");
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
        let first_row = ["Viscosity", "Rating", "Weighted", "Class"].map(|s| s.to_string());

        let headers = build_a1_headers(&first_row);
        // should be lower-cased as `viscosity`
        assert_eq!(headers.first().unwrap(), "a");
        // the column should be d.
        assert_eq!(headers.get(3).unwrap(), "d");
    }

    #[test]
    fn test_headers_c01_override() {
        // build header row with 200 sequential alphanumeric values
        let first_row: Vec<String> = (0..200)
            .map(|x| {
                [
                    char::from_u32(65 + (x % 26)).unwrap_or('_').to_string(),
                    (x * 3).to_string(),
                ]
                .concat()
            })
            .collect();

        let headers = build_c01_headers(&first_row);
        // the column should be c0001
        assert_eq!(headers.first().unwrap(), "c001");
        // the column should be c0004
        assert_eq!(headers.get(3).unwrap(), "c004");
    }
}
