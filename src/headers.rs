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
            if let Some(segment) = &col.key {
                let k_str = segment.to_string();
                let h_key = if headers.contains(&k_str) {
                    to_padded_col_suffix(&k_str, h_index, num_cols)
                } else {
                    k_str
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

/// Combines `header_row_span` consecutive raw header rows into the single effective
/// header row `build_header_keys`/`natural_column_keys` expect, for spreadsheets whose
/// header spans more than one row (e.g. a merged "2015"/"2025" year row with a "Female"/
/// "Male" sub-label row underneath).
///
/// Each row is forward-filled *independently* first: a blank cell inherits the nearest
/// non-blank value to its left within that same row, which is what makes a merged cell
/// work without needing any merge-range metadata from calamine at all -- calamine (like
/// the underlying xlsx/CSV data) only ever reports a merged cell's value in its top-left
/// position, leaving the rest of the merge blank, and that's indistinguishable from an
/// ordinary blank cell that just happens to be empty. Forward-fill produces the correct
/// result either way: a genuine merge "spreads" its one value across the columns it
/// visually spans, and an incidentally-blank cell (nothing to its left either) stays
/// blank rather than inheriting something from a different, unrelated column.
///
/// Then, down each column, every row's (now filled) value is joined with `_` -- blank
/// values (a leading blank with nothing to its left to inherit) are skipped entirely
/// rather than leaving a stray separator, so a column where only one row actually
/// contributes text still gets a clean single-segment key.
pub fn combine_header_rows(rows: &[Vec<String>]) -> Vec<String> {
    let num_cols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let filled: Vec<Vec<String>> = rows.iter().map(|row| forward_fill_row(row, num_cols)).collect();
    (0..num_cols)
        .map(|col_index| {
            filled
                .iter()
                .filter_map(|row| row.get(col_index))
                .filter(|v| !v.is_empty())
                .cloned()
                .collect::<Vec<String>>()
                .join("_")
        })
        .collect()
}

fn forward_fill_row(row: &[String], num_cols: usize) -> Vec<String> {
    let mut result = Vec::with_capacity(num_cols);
    let mut carry: Option<&str> = None;
    for i in 0..num_cols {
        let cell = row.get(i).map(|s| s.trim()).unwrap_or("");
        if !cell.is_empty() {
            carry = Some(cell);
            result.push(cell.to_string());
        } else {
            result.push(carry.unwrap_or("").to_string());
        }
    }
    result
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

    use crate::{DateTimeMode, Format};

    use super::*;

    #[test]
    fn test_cell_letters_1() {
        assert_eq!(to_a1_col_key(26), "aa");
    }

    fn strs(vals: &[&str]) -> Vec<String> {
        vals.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_combine_header_rows_forward_fills_a_single_merged_block() {
        // A1:C1 merged "2015" -- calamine reports it only in A1, B1/C1 blank.
        let rows = vec![strs(&["2015", "", ""]), strs(&["North", "Midlands", "South"])];
        assert_eq!(
            combine_header_rows(&rows),
            strs(&["2015_North", "2015_Midlands", "2015_South"])
        );
    }

    #[test]
    fn test_combine_header_rows_handles_two_merged_blocks_in_the_same_row() {
        let rows = vec![
            strs(&["2015", "", "", "2025", "", ""]),
            strs(&["North", "Midlands", "South", "North", "Midlands", "South"]),
        ];
        assert_eq!(
            combine_header_rows(&rows),
            strs(&[
                "2015_North", "2015_Midlands", "2015_South",
                "2025_North", "2025_Midlands", "2025_South"
            ])
        );
    }

    #[test]
    fn test_combine_header_rows_leaves_a_leading_blank_with_nothing_to_inherit_empty() {
        // country_code has no row-1 label at all (nothing merged over it) and no row-2
        // sub-label either -- both rows contribute nothing, not a stray "_" separator.
        let rows = vec![
            strs(&["", "2015", ""]),
            strs(&["", "North", "South"]),
        ];
        assert_eq!(combine_header_rows(&rows), strs(&["", "2015_North", "2015_South"]));
    }

    #[test]
    fn test_combine_header_rows_skips_a_row_that_contributes_nothing_for_one_column() {
        // country_code (column 0) only ever gets a value from row 1 -- row 2 is blank
        // for it and shouldn't leave a trailing "_".
        let rows = vec![strs(&["country code", "2015", ""]), strs(&["", "North", "South"])];
        assert_eq!(combine_header_rows(&rows), strs(&["country code", "2015_North", "2015_South"]));
    }

    #[test]
    fn test_combine_header_rows_with_a_single_row_is_a_no_op() {
        // header_row_span == 1 (the default) -- output is identical to the input.
        let rows = vec![strs(&["id", "name", "score"])];
        assert_eq!(combine_header_rows(&rows), strs(&["id", "name", "score"]));
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
            Column::from_source_key_with_format("weight_kg", Some("weight"), Format::Integer, None, DateTimeMode::Full, false),
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
            Column::from_source_key_with_format("Weight Kg", Some("weight"), Format::Auto, None, DateTimeMode::Full, false),
        ];
        let resolved = resolve_columns(&overrides, &natural_keys);
        assert_eq!(resolved[0].key_name(), "weight");
    }

    #[test]
    fn test_resolve_columns_unmatched_source_key_is_a_no_op() {
        let first_row = ["full_name", "height_cm"].map(|s| s.to_string());
        let natural_keys = natural_column_keys(&first_row, &FieldNameMode::AutoA1);
        let overrides = vec![
            Column::from_source_key_with_format("nonexistent_field", Some("oops"), Format::Auto, None, DateTimeMode::Full, false),
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
            Column::from_key_ref_with_format(None, Format::Float, None, DateTimeMode::Full, false),
            Column::from_key_ref_with_format(
                Some("points"),
                Format::Decimal(3),
                None,
                DateTimeMode::Full,
                false,
            ),
            Column::from_key_ref_with_format(Some("adjusted"), Format::Float, None, DateTimeMode::Full, false),
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
