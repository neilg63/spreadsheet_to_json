use calamine::{open_workbook_auto, Data, Reader, Sheets};
use csv::{ReaderBuilder, StringRecord};
use heck::ToSnakeCase;
use indexmap::IndexMap;
use serde_json::{Number, Value};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::str::FromStr;

use crate::data_set::*;
use crate::detect::{resolve_header_and_data_rows, DETECT_SAMPLE_SIZE};
use crate::error::GenericError;
use alphanumeric::*;
use simple_string_patterns::SimpleMatch;
use crate::headers::*;
use crate::helpers::float_value;
use crate::helpers::string_value;
use is_truthy::*;
use crate::round_decimal::RoundDecimal;
use crate::DateTimeMode;
use crate::Extension;
use crate::Format;
use crate::OptionSet;
use crate::PathData;
use crate::RowOptionSet;
use fuzzy_datetime::{fuzzy_to_date_string, fuzzy_to_datetime_string_opts, iso_fuzzy_to_date_string, iso_fuzzy_to_datetime_string};

/// Callback invoked once per row when saving asynchronously (e.g. --deferred mode)
pub type SaveRowFn = Box<dyn Fn(IndexMap<String, Value>) -> Result<(), GenericError> + Send + Sync>;

/// Output the result set with captured rows (up to the maximum allowed) directly.
/// This is now synchronous and calls the asynchronous function using a runtime.
pub fn process_spreadsheet_direct(opts: &OptionSet) -> Result<ResultSet, GenericError> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(process_spreadsheet_core(opts, None, None))
}

/// Output the result set with captured rows (up to the maximum allowed) immediately.
/// Use this in an async function using the tokio runtime if you direct results
/// without a save callback
pub async fn process_spreadsheet_immediate(opts: &OptionSet) -> Result<ResultSet, GenericError> {
    process_spreadsheet_core(opts, None, None).await
}

#[deprecated(
    since = "1.0.6",
    note = "This function is a wrapper for the renamed function `process_spreadsheet_inline`"
)]
pub async fn render_spreadsheet_direct(opts: &OptionSet) -> Result<ResultSet, GenericError> {
    process_spreadsheet_core(opts, None, None).await
}

/// Output the result set with deferred row saving and optional output reference
pub async fn process_spreadsheet_async(
    opts: &OptionSet,
    save_func: SaveRowFn,
    out_ref: Option<&str>,
) -> Result<ResultSet, GenericError> {
    process_spreadsheet_core(opts, Some(save_func), out_ref).await
}

/// Output the result set with captured rows (up to the maximum allowed) directly.
/// with optional asynchronous row save method and output reference
pub async fn process_spreadsheet_core(
    opts: &OptionSet,
    save_opt: Option<SaveRowFn>,
    out_ref: Option<&str>,
) -> Result<ResultSet, GenericError> {
    if let Some(filepath) = opts.path.clone() {
        let path = Path::new(&filepath);
        if !path.exists() {
            #[allow(dead_code)]
            return Err(GenericError("file_unavailable"));
        }
        let path_data = PathData::new(path);
        if path_data.is_valid() {
            if path_data.use_calamine() {
                read_workbook_core(&path_data, opts, save_opt, out_ref).await
            } else {
                read_csv_core(&path_data, opts, save_opt, out_ref).await
            }
        } else {
            Err(GenericError("unsupported_format"))
        }
    } else {
        Err(GenericError("no_filepath_specified"))
    }
}

#[deprecated(
    since = "1.0.6",
    note = "This function is a wrapper for the renamed function `process_spreadsheet_core`"
)]
pub async fn render_spreadsheet_core(
    opts: &OptionSet,
    save_opt: Option<SaveRowFn>,
    out_ref: Option<&str>,
) -> Result<ResultSet, GenericError> {
    process_spreadsheet_core(opts, save_opt, out_ref).await
}

/// Parse spreadsheets with an optional callback method to save rows asynchronously and an optional output reference
/// that may be a file name or database identifier
pub async fn read_workbook_core<'a>(
    path_data: &PathData<'a>,
    opts: &OptionSet,
    save_opt: Option<SaveRowFn>,
    out_ref: Option<&str>,
) -> Result<ResultSet, GenericError> {
    if let Ok(mut workbook) = open_workbook_auto(path_data.path()) {
        let max_rows = opts.max_rows();
        let (selected_names, sheet_names, _sheet_indices) =
            match_sheet_name_and_index(&mut workbook, opts);

        if !selected_names.is_empty() {
            let info = WorkbookInfo::new(path_data, &selected_names, &sheet_names);

            if opts.multimode() {
                read_multiple_worksheets(&mut workbook, &sheet_names, opts, &info, max_rows).await
            } else {
                let sheet_ref = &selected_names[0];
                read_single_worksheet(workbook, sheet_ref, opts, &info, save_opt, out_ref).await
            }
        } else {
            Err(GenericError("workbook_with_no_sheets"))
        }
    } else {
        Err(GenericError("cannot_open_workbook"))
    }
}

/// Read multiple worksheets from a workbook in preview mode.
async fn read_multiple_worksheets(
    workbook: &mut Sheets<BufReader<File>>,
    sheet_names: &[String],
    opts: &OptionSet,
    info: &WorkbookInfo,
    max_rows: usize,
) -> Result<ResultSet, GenericError> {
    let mut sheets: Vec<SheetDataSet> = vec![];
    let capture_rows = opts.capture_rows();
    for (sheet_index, sheet_ref) in sheet_names.iter().enumerate() {
        let range = workbook.worksheet_range(&sheet_ref.clone())?;
        let mut headers: Vec<String> = vec![];
        let mut has_headers = false;
        let mut rows: Vec<IndexMap<String, Value>> =
            Vec::with_capacity(if capture_rows { max_rows } else { 0 });
        let mut row_index = 0;
        let detected = resolve_header_and_data_rows(opts, || {
            range.rows().take(DETECT_SAMPLE_SIZE)
                .map(|row| row.iter().map(|c| c.to_string()).collect())
                .collect()
        });
        let first_data_row_index = detected.data_index;
        let capture_headers = detected.header_index.is_some();
        let header_row_index = detected.header_index.unwrap_or(0);
        let mut col_keys: Vec<String> = vec![];
        let columns = if sheet_index == 0 {
            opts.rows.columns.clone()
        } else {
            vec![]
        };
        let mut resolved_row_opts = opts.rows.clone();
        let match_header_row_below = capture_headers && header_row_index > 0;
        if capture_headers {
            if let Some(first_row) = range.headers() {
                let natural_keys = natural_column_keys(&first_row, &opts.field_mode);
                let resolved_columns = resolve_columns(&columns, &natural_keys);
                headers = build_header_keys(&first_row, &resolved_columns, &opts.field_mode);
                resolved_row_opts.columns = resolved_columns;
                has_headers = !match_header_row_below;
                col_keys = first_row;
            }
        } else {
            let num_cols = range.get_size().1;
            let blank = vec![String::new(); num_cols];
            let natural_keys = natural_column_keys(&blank, &opts.field_mode);
            let resolved_columns = resolve_columns(&columns, &natural_keys);
            headers = build_header_keys(&blank, &resolved_columns, &opts.field_mode.forced_fallback());
            resolved_row_opts.columns = resolved_columns;
            has_headers = true;
        }
        let total = range.get_size().0;
        if capture_rows || match_header_row_below {
            let max_row_count = if capture_rows {
                max_rows
            } else {
                header_row_index + 2
            };
            let max_take = if total < max_row_count {
                total
            } else {
                max_row_count + 1
            };
            for row in range.rows().take(max_take) {
                if row_index > max_row_count {
                    break;
                }
                if match_header_row_below && row_index == header_row_index {
                    let h_row = row
                        .iter()
                        .map(|c| c.to_string().to_snake_case())
                        .collect::<Vec<String>>();
                    let natural_keys = natural_column_keys(&h_row, &opts.field_mode);
                    let resolved_columns = resolve_columns(&columns, &natural_keys);
                    headers = build_header_keys(&h_row, &resolved_columns, &opts.field_mode);
                    resolved_row_opts.columns = resolved_columns;
                    has_headers = true;
                } else if (has_headers || !capture_headers) && capture_rows
                    && row_index >= first_data_row_index {
                    let is_real_data = if capture_headers {
                        let raw_values: Vec<String> = row.iter().map(|c| c.to_string()).collect();
                        is_not_header_row(&raw_values, row_index, &col_keys)
                    } else {
                        true
                    };
                    if is_real_data {
                        let row_map = workbook_row_to_map(row, &resolved_row_opts, &headers);
                        rows.push(row_map);
                    }
                }
                row_index += 1;
            }
        }
        sheets.push(SheetDataSet::new(sheet_ref, &headers, &rows, total));
    }
    Ok(ResultSet::from_multiple(&sheets, info, opts))
}

/// Read a single worksheet from a workbook in immediate (sync) or asycnhronous modes
pub async fn read_single_worksheet(
    mut workbook: Sheets<BufReader<File>>,
    sheet_ref: &str,
    opts: &OptionSet,
    info: &WorkbookInfo,
    save_opt: Option<SaveRowFn>,
    out_ref: Option<&str>,
) -> Result<ResultSet, GenericError> {
    let range = workbook.worksheet_range(sheet_ref)?;
    let capture_rows = opts.capture_rows();
    let columns = opts.rows.columns.clone();
    let max_rows = opts.max_rows();
    let mut headers: Vec<String> = vec![];
    let mut col_keys: Vec<String> = vec![];
    let mut has_headers = false;
    let mut rows: Vec<IndexMap<String, Value>> =
        Vec::with_capacity(if capture_rows { max_rows } else { 0 });
    let mut row_index = 0;
    let detected = resolve_header_and_data_rows(opts, || {
        range.rows().take(DETECT_SAMPLE_SIZE)
            .map(|row| row.iter().map(|c| c.to_string()).collect())
            .collect()
    });
    let first_data_row_index = detected.data_index;
    // No row is consumed as a header-text source for --omit-header, *or* when detection
    // found no confident header row at all (see DetectedRows::header_index) -- both
    // cases fall back to A1/C01-style names instead of deriving them from row text.
    let capture_headers = detected.header_index.is_some();
    let header_row_index = detected.header_index.unwrap_or(0);
    let match_header_row_below = capture_headers && header_row_index > 0;
    let mut resolved_row_opts = opts.rows.clone();

    if capture_headers {
        if let Some(first_row) = range.headers() {
            let natural_keys = natural_column_keys(&first_row, &opts.field_mode);
            let resolved_columns = resolve_columns(&columns, &natural_keys);
            headers = build_header_keys(&first_row, &resolved_columns, &opts.field_mode);
            resolved_row_opts.columns = resolved_columns;
            has_headers = !match_header_row_below;
            col_keys = first_row;
        }
    } else {
        let num_cols = range.get_size().1;
        let blank = vec![String::new(); num_cols];
        let natural_keys = natural_column_keys(&blank, &opts.field_mode);
        let resolved_columns = resolve_columns(&columns, &natural_keys);
        headers = build_header_keys(&blank, &resolved_columns, &opts.field_mode.forced_fallback());
        resolved_row_opts.columns = resolved_columns;
        has_headers = true;
    }
    let total = range.get_size().0;
    if capture_rows || match_header_row_below {
        let max_row_count = if capture_rows {
            max_rows
        } else {
            header_row_index + 2
        };
        let max_take = if total < max_row_count {
            total
        } else {
            max_row_count + 1
        };
        for row in range.rows().take(max_take) {
            if row_index > max_row_count {
                break;
            }
            if match_header_row_below && row_index == header_row_index {
                let h_row = row
                    .iter()
                    .map(|c| c.to_string().to_snake_case())
                    .collect::<Vec<String>>();
                let natural_keys = natural_column_keys(&h_row, &opts.field_mode);
                let resolved_columns = resolve_columns(&columns, &natural_keys);
                headers = build_header_keys(&h_row, &resolved_columns, &opts.field_mode);
                resolved_row_opts.columns = resolved_columns;
                has_headers = true;
            } else if (has_headers || !capture_headers) && capture_rows
                && row_index >= first_data_row_index {
                // only capture rows if headers are either omitted or have already been captured
                let is_real_data = if capture_headers {
                    let raw_values: Vec<String> = row.iter().map(|c| c.to_string()).collect();
                    is_not_header_row(&raw_values, row_index, &col_keys)
                } else {
                    // no header row was consumed, so there's no header text to
                    // self-exclude a duplicate row against
                    true
                };
                if is_real_data {
                    let row_map = workbook_row_to_map(row, &resolved_row_opts, &headers);
                    rows.push(row_map);
                }
            }
            row_index += 1;
        }
    }
    if let Some(save_method) = save_opt {
        // Skip everything before first_data_row_index (the header row itself, and any
        // title/notes/gap rows above it) -- this used to just stream from the true start
        // of the sheet regardless of header_row_index/data_row_index, silently exporting
        // notes rows as bogus data records.
        let mut save_count: usize = 0;
        for (idx, row) in range.rows().enumerate() {
            if save_count >= max_rows {
                break;
            }
            if idx < first_data_row_index {
                continue;
            }
            let is_real_data = if capture_headers {
                let raw_values: Vec<String> = row.iter().map(|c| c.to_string()).collect();
                is_not_header_row(&raw_values, idx, &col_keys)
            } else {
                true
            };
            if is_real_data {
                let row_map = workbook_row_to_map(row, &resolved_row_opts, &headers);
                save_method(row_map)?;
                save_count += 1;
            }
        }
    }

    let ds = DataSet::from_count_and_rows(total, rows, opts);
    Ok(ResultSet::new(info, &headers, ds, opts, out_ref, detected.header_index, first_data_row_index))
}

/// Process a CSV/TSV file asynchronously with an optional row save method
/// and output reference (file or database table reference)
///
/// Reads with `has_headers(false)` and drives header/gap/data classification manually by
/// 0-based line index (mirroring the calamine path in `read_single_worksheet`) rather than
/// relying on the `csv` crate's own implicit "always skip the first line" behavior --
/// needed to honor `header_row`/`data_row_index` (including the case where they're equal:
/// a CSV with predefined/external headers where no line is actually consumed as a header,
/// e.g. `omit_header` with a fixed schema via `--keys`).
pub async fn read_csv_core<'a>(
    path_data: &PathData<'a>,
    opts: &OptionSet,
    save_opt: Option<SaveRowFn>,
    out_ref: Option<&str>,
) -> Result<ResultSet, GenericError> {
    let separator = match path_data.mode() {
        Extension::Tsv => b't',
        _ => b',',
    };
    if let Ok(mut rdr) = ReaderBuilder::new()
        .delimiter(separator)
        .has_headers(false)
        // Notes/title rows before the real header (header_row > 0) commonly have a
        // different field count than the data rows below them -- without this, the
        // csv crate rejects every record as malformed once row 0's width doesn't match
        // the rest of the file.
        .flexible(true)
        .from_path(path_data.path())
    {
        let capture_rows = opts.capture_rows();
        let max_line_usize = opts.max_rows();
        // Sampling (when actually needed for detection) opens a fresh, short-lived reader
        // rather than reusing `rdr` -- csv::Reader is a moving cursor, so peeking ahead on
        // the same reader would consume records the main pass below still needs.
        let detected = resolve_header_and_data_rows(opts, || {
            let mut sample_rows = Vec::new();
            if let Ok(mut sample_rdr) = ReaderBuilder::new()
                .delimiter(separator)
                .has_headers(false)
                .flexible(true)
                .from_path(path_data.path())
            {
                for record in sample_rdr.records().take(DETECT_SAMPLE_SIZE).flatten() {
                    sample_rows.push(record.iter().map(|s| s.to_string()).collect());
                }
            }
            sample_rows
        });
        let first_data_row_index = detected.data_index;
        // No line is a header source for --omit-header, *or* when detection found no
        // confident header row at all (see DetectedRows::header_index) -- both fall
        // back to lazily-built A1/C01-style names below.
        let capture_header = detected.header_index.is_some();
        let header_row_index = detected.header_index.unwrap_or(0);

        let mut rows: Vec<IndexMap<String, Value>> =
            Vec::with_capacity(if capture_rows { max_line_usize } else { 0 });
        let mut headers: Vec<String> = vec![];
        let mut resolved_row_opts = opts.rows.clone();
        // With omit_header, no line is ever a header source -- fallback (A1/C01) keys are
        // derived once, lazily, from the first eligible data row's column count.
        let mut fallback_keys_built = false;

        let mut total: usize = 0;
        let mut line_count: usize = 0;
        let mut row_index: usize = 0;

        for result in rdr.records() {
            let Ok(record) = result else {
                row_index += 1;
                continue;
            };
            // "total"/num_rows is a structural line count for the whole file, matching
            // the calamine path's range.get_size().0 -- it includes the header row (and
            // any skipped gap rows), not just rows that end up classified as data.
            total += 1;

            if capture_header && row_index == header_row_index {
                let raw: Vec<String> = record.iter().map(|s| s.to_string()).collect();
                let natural_keys = natural_column_keys(&raw, &opts.field_mode);
                let resolved_columns = resolve_columns(&opts.rows.columns, &natural_keys);
                headers = build_header_keys(&raw, &resolved_columns, &opts.field_mode);
                resolved_row_opts.columns = resolved_columns;
                row_index += 1;
                continue;
            }

            if row_index < first_data_row_index {
                row_index += 1;
                continue;
            }

            if !capture_header && !fallback_keys_built {
                let blank: Vec<String> = record.iter().map(|_| String::new()).collect();
                let resolved_columns = resolve_columns(&opts.rows.columns, &natural_column_keys(&blank, &opts.field_mode));
                headers = build_header_keys(&blank, &resolved_columns, &opts.field_mode.forced_fallback());
                resolved_row_opts.columns = resolved_columns;
                fallback_keys_built = true;
            }

            if capture_rows {
                if line_count < max_line_usize {
                    if let Some(row) = csv_row_result_to_values(Ok(record), &resolved_row_opts) {
                        rows.push(to_index_map(&row, &headers));
                        line_count += 1;
                    }
                }
            } else if let Some(save_method) = save_opt.as_ref() {
                if let Some(row) = csv_row_result_to_values(Ok(record), &resolved_row_opts) {
                    let row_map = to_index_map(&row, &headers);
                    save_method(row_map)?;
                }
            }
            row_index += 1;
        }
        let info = WorkbookInfo::simple(path_data);
        let ds = DataSet::from_count_and_rows(total, rows, opts);
        Ok(ResultSet::new(&info, &headers, ds, opts, out_ref, detected.header_index, first_data_row_index))
    } else {
        let error_msg = match path_data.ext() {
            Extension::Tsv => "unreadable_tsv_file",
            _ => "unreadable_csv_file",
        };
        Err(GenericError(error_msg))
    }
}

// Convert an array of row data to an IndexMap of serde_json::Value objects
fn workbook_row_to_map(
    row: &[Data],
    opts: &RowOptionSet,
    headers: &[String],
) -> IndexMap<String, Value> {
    to_index_map(&workbook_row_to_values(row, opts), headers)
}

// Convert an array of row data to a vector of serde_json::Value objects
fn workbook_row_to_values(row: &[Data], opts: &RowOptionSet) -> Vec<Value> {
    row.iter()
        .enumerate()
        .map(|(c_index, cell)| workbook_cell_to_value(cell, opts, c_index))
        .collect()
}

/// Convert a spreadsheet data cell to a polymorphic serde_json::Value object
fn workbook_cell_to_value(cell: &Data, opts: &RowOptionSet, c_index: usize) -> Value {
    let col = opts.column(c_index);
    let format = col.map_or(Format::Auto, |c| c.format.to_owned());
    let def_val = col.and_then(|c| c.default.clone());
    let col_mode = col.map_or(DateTimeMode::Full, |c| c.datetime_mode);

    let mode = resolve_datetime_mode(&format, col_mode, opts.datetime_mode);

    match cell {
        Data::Int(i) => Value::Number(Number::from_i128(*i as i128).unwrap()),
        Data::Float(f) => process_float_value(*f, format, def_val),
        Data::DateTimeIso(d) => process_iso_datetime_value(d, def_val, mode),
        Data::DateTime(d) => process_excel_datetime_value(d, def_val, mode),
        Data::Bool(b) => Value::Bool(*b),
        Data::String(s) => process_string_value(s, format, def_val),
        Data::Empty => def_val.unwrap_or(Value::Null),
        _ => Value::String(cell.to_string()),
    }
}

/// A column's own Format::Date/Format::Time/Format::Hm/Format::DateTime/
/// Format::DateTimeSimple override takes precedence over everything else, since it
/// forces date/time interpretation regardless of the cell's native type. Next is the
/// column's own `datetime_mode` (only meaningful on a Format::Auto column, restricted to
/// cells that are already genuine datetimes -- see `Column::datetime_mode`'s doc
/// comment). Anything else falls back to the row-wide default. No override anywhere
/// means the full datetime.
fn resolve_datetime_mode(format: &Format, col_mode: DateTimeMode, row_mode: DateTimeMode) -> DateTimeMode {
    match format {
        Format::Date => DateTimeMode::DateOnly,
        Format::Time => DateTimeMode::TimeOnly,
        Format::Hm => DateTimeMode::HmOnly,
        Format::DateTime => DateTimeMode::Full,
        Format::DateTimeSimple => DateTimeMode::Simple,
        _ if col_mode != DateTimeMode::Full => col_mode,
        _ => row_mode,
    }
}

fn process_float_value(value: f64, format: Format, def_val: Option<Value>) -> Value {
    match format {
        Format::Integer => Value::Number(Number::from_i128(value as i128).unwrap()),
        Format::Boolean => Value::Bool(value >= 1.0),
        Format::Text => Value::String(value.to_string()),
        Format::Decimal(places) => float_value(value.round_decimal(places)),
        Format::Time | Format::Hm => decimal_to_hm_string(value)
            .map_or_else(|| def_val.unwrap_or(Value::Null), Value::String),
        _ => Value::Number(Number::from_f64(value).unwrap()),
    }
}

fn process_excel_datetime_value(
    datetime: &calamine::ExcelDateTime,
    def_val: Option<Value>,
    mode: DateTimeMode,
) -> Value {
    // Excel has no true time-only type -- a cell formatted as plain "hh:mm" (not the
    // bracketed "[h]:mm:ss" duration format) is really a full datetime serial with zero
    // elapsed days, which calamine converts by landing on its epoch ("1899-12-31" in the
    // 1900 date system). Carrying that placeholder date through to a full ISO datetime
    // string would misrepresent a genuine time-of-day value as if it were a real date,
    // so a cell with no real date component (serial < 1.0) is auto-rendered as a bare
    // time even without an explicit Format::Time/--time-only request -- for both Full
    // and Simple modes, since Simple is still "the whole datetime", just reformatted.
    let auto_time_only = matches!(mode, DateTimeMode::Full | DateTimeMode::Simple) && datetime.as_f64() < 1.0;
    datetime.as_datetime().map_or_else(
        || def_val.unwrap_or(Value::Null),
        |dt| {
            // Milliseconds are only meaningful in the default Full mode's genuine
            // full-datetime output, kept for JS-interop compatibility; every other mode
            // -- including Full/Simple's own bare-time fallback above -- renders plain
            // "HH:MM:SS", since a value that's already been reduced to seconds-only
            // precision (or came from an Excel "hh:mm"-formatted cell with no seconds
            // at all) gains nothing from a trailing ".000".
            let formatted_date = match mode {
                DateTimeMode::DateOnly => dt.format("%Y-%m-%d").to_string(),
                DateTimeMode::TimeOnly => dt.format("%H:%M:%S").to_string(),
                DateTimeMode::HmOnly => dt.format("%H:%M").to_string(),
                DateTimeMode::Simple if auto_time_only => dt.format("%H:%M:%S").to_string(),
                DateTimeMode::Simple => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
                DateTimeMode::Full if auto_time_only => dt.format("%H:%M:%S").to_string(),
                DateTimeMode::Full => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            };
            Value::String(formatted_date)
        },
    )
}

fn process_iso_datetime_value(dt_str: &str, def_val: Option<Value>, mode: DateTimeMode) -> Value {
    match mode {
        DateTimeMode::DateOnly => iso_fuzzy_to_date_string(dt_str)
            .map_or_else(|| def_val.unwrap_or(Value::Null), Value::String),
        DateTimeMode::TimeOnly => iso_fuzzy_to_datetime_string(dt_str)
            .and_then(|full| extract_time_portion(&full, false))
            .map_or_else(|| def_val.unwrap_or(Value::Null), Value::String),
        DateTimeMode::HmOnly => iso_fuzzy_to_datetime_string(dt_str)
            .and_then(|full| extract_time_portion(&full, true))
            .map_or_else(|| def_val.unwrap_or(Value::Null), Value::String),
        DateTimeMode::Simple => iso_fuzzy_to_datetime_string(dt_str)
            .map(|full| simplify_datetime_string(&full))
            .map_or_else(|| def_val.unwrap_or(Value::Null), Value::String),
        DateTimeMode::Full => iso_fuzzy_to_datetime_string(dt_str)
            .map_or_else(|| def_val.unwrap_or(Value::Null), Value::String),
    }
}

/// Extracts just the time-of-day portion from a full ISO-8601 datetime string like
/// "2023-06-15T10:17:00.000Z" -- the shape iso_fuzzy_to_datetime_string always produces
/// -- as "HH:MM:SS" or, when `hm_only`, just "HH:MM"; milliseconds are always dropped,
/// matching process_excel_datetime_value's native-cell equivalent. fuzzy-datetime itself
/// has no time-only concept by design (it stays scoped to date/datetime completion and
/// order-guessing); this is purely string surgery on its output for
/// Format::Time/Format::Hm/--time-only/--hm-only, not a new parsing capability.
fn extract_time_portion(full_datetime_str: &str, hm_only: bool) -> Option<String> {
    let time_part = full_datetime_str.split('T').nth(1)?;
    let time_part = time_part.trim_end_matches('Z');
    let len = if hm_only { 5 } else { 8 };
    time_part.get(0..len).map(str::to_string)
}

/// Strips the trailing ".mmmZ" milliseconds-and-Z suffix from a full ISO-8601 datetime
/// string like "2023-06-15T10:17:00.000Z", leaving "2023-06-15T10:17:00" -- for
/// Format::DateTimeSimple/--simple, where the millisecond precision and JS-interop-style
/// trailing Z (both there for the default Full mode) are unwanted noise.
fn simplify_datetime_string(full_datetime_str: &str) -> String {
    let trimmed = full_datetime_str.trim_end_matches('Z');
    match trimmed.split_once('.') {
        Some((base, _)) => base.to_string(),
        None => trimmed.to_string(),
    }
}

/// Parses a plain-text date string with fuzzy-datetime's own order/separator guessing
/// enabled (`date_opts: None`), rather than `iso_fuzzy_to_date_string`'s forced YMD + '-'
/// assumption. Text cells and CSV values commonly use DMY or MDY order with '/' as the
/// separator -- e.g. "19/07/2026" -- which the forced function rejects outright (it never
/// even reaches order-guessing, since an explicit splitter was already given). The output
/// shape is always the same canonical "YYYY-MM-DD" regardless of the input's order or
/// separator -- only what's *accepted* as input is more flexible, not what's produced.
fn guess_date_string(value: &str) -> Option<String> {
    fuzzy_to_date_string(value, None)
}

/// Datetime equivalent of `guess_date_string` -- see its doc comment. Always produces the
/// canonical "YYYY-MM-DDTHH:MM:SS.mmmZ" shape `iso_fuzzy_to_datetime_string` also
/// produces, just with flexible input-side order/separator guessing instead of a forced
/// YMD + '-' assumption.
fn guess_datetime_string(value: &str) -> Option<String> {
    fuzzy_to_datetime_string_opts(value, 'T', None, Some(':'), true)
}

/// Recognizes a string that's already plausibly just a bare time -- colon-separated
/// ("11:39") or dot-separated ("12.30", the common case where a user typed a time with
/// '.' instead of ':' and the cell was explicitly text-formatted, so Excel/Sheets never
/// got the chance to convert it to a float; a string never loses the trailing-zero
/// information a float would, so unlike decimal_to_hm_string below, no numeric
/// reconstruction is needed here, just a separator swap). Only ever consulted as a
/// fallback once iso_fuzzy_to_datetime_string has already failed to parse the string as
/// a full datetime (see the callers below), so a genuine full datetime string -- which
/// always succeeds there first -- never reaches this at all; there's no risk of
/// misclassifying one.
///
/// '.' is swapped for ':' first (a no-op if there wasn't one), then the individual
/// components are extracted directly as integers via `to_numbers::<u8>()` -- no regex,
/// no manual splitting -- and accepted as a plausible 2- or 3-part hh:mm(:ss) shape,
/// reformatted with zero-padding. Minutes/seconds are bounded to < 60 (always true
/// regardless of what the hours represent), but hours are *not* capped at < 24: a "time"
/// column is just as often a duration (elapsed hours, a race/video/timesheet total) as a
/// time-of-day, and durations routinely exceed 23 hours (e.g. "27:45:00") -- there's no
/// way to tell which one a given column means from the value alone, so this doesn't
/// guess and reject perfectly valid durations. An explicit Format::Time/Format::Hm
/// override is still "I know this is a time" -- a well-formed hh:mm(:ss) shape is
/// trusted -- but a clearly-implausible one ("11:60") or anything that isn't 2-3 numeric
/// parts is rejected rather than passed through as a bogus result.
///
/// A trailing AM/PM marker is the one exception: it's detected up front via
/// `ends_with_ci`, and once detected the hour is reinterpreted as 12-hour-clock (1-12)
/// rather than rejected outright -- "12:45am" is 00:45, "2:30pm" is 14:30, "12:00pm" (noon)
/// stays 12. An hour outside 1-12 alongside an am/pm marker ("14:00pm") isn't genuine
/// 12-hour notation at all -- the 12-hour clock never produces those values -- so rather
/// than treating it as contradictory and rejecting it, the hour is trusted as already
/// being literal 24-hour and the (redundant) suffix is simply dropped: "14:00pm" is
/// "14:00". This is consistent, if non-standard, data entry (some spreadsheet sources
/// append am/pm out of habit regardless of hour), not the malformed/mistaken-type case
/// this function otherwise sanitizes against.
///
/// Any OTHER letters -- a duration-unit suffix ("2h45m"), stray text -- still reject
/// outright before ever calling to_numbers(). `to_numbers()` silently discards non-digit
/// characters, so without this guard "2h45m" would extract as [2, 45] and format as
/// "02:45" -- not a rejection, a *wrong* answer indistinguishable from a correct one.
fn parse_bare_time_string(value: &str) -> Option<String> {
    let has_am_suffix = value.ends_with_ci("am");
    let has_pm_suffix = !has_am_suffix && value.ends_with_ci("pm");
    let is_12_hour = has_am_suffix || has_pm_suffix;

    if !is_12_hour && value.has_alphabetic() {
        return None;
    }

    // Strip the am/pm suffix itself before extraction -- to_numbers() happens to treat
    // any letter as a boundary and skip it anyway, but stripping explicitly here doesn't
    // depend on that being true of every input shape or future version of to_numbers().
    let numeric_part = if is_12_hour {
        value[..value.len() - 2].trim_end()
    } else {
        value
    };

    let numbers: Vec<u8> = numeric_part.replace('.', ":").to_numbers();

    let resolve_hour = |hrs: u8| -> u8 {
        if !is_12_hour || !(1..=12).contains(&hrs) {
            // Not ambiguous 12-hour notation -- either no am/pm marker at all, or an
            // hour a real 12-hour clock could never produce -- so the hour is used as
            // literally given and any am/pm marker present is redundant, not corrective.
            return hrs;
        }
        match (has_am_suffix, hrs) {
            (true, 12) => 0,        // 12am -> midnight
            (true, _) => hrs,       // 1am-11am unchanged
            (false, 12) => 12,      // 12pm -> noon, unchanged
            (false, _) => hrs + 12, // 1pm-11pm -> +12
        }
    };

    match numbers.as_slice() {
        [hrs, mins] if *mins < 60 => Some(format!("{:02}:{:02}", resolve_hour(*hrs), mins)),
        [hrs, mins, secs] if *mins < 60 && *secs < 60 => {
            Some(format!("{:02}:{:02}:{:02}", resolve_hour(*hrs), mins, secs))
        }
        _ => None,
    }
}

/// Reconstructs a sexagesimal "HH:MM" time from a plain decimal number, for the common
/// real-world case where a spreadsheet app silently turned a user's dot-typed time entry
/// (e.g. typing "12.30", meaning 12:30) into a bare float with the trailing zero
/// stripped ("12.3") -- Excel/Sheets have no dedicated time type for a dot-typed value,
/// so it's read as a plain decimal number instead. Used by process_float_value for
/// Format::Time/Format::Hm on a genuine (native, non-string) numeric cell, which is
/// presumed to mean exactly this, not a generic decimal-to-time conversion (0.3 of an
/// hour is not 30 minutes -- the digits after the point are read literally as MM, never
/// scaled by 60). The string-cell equivalent is simpler and doesn't need this: see
/// parse_bare_time_string's doc comment for why.
///
/// The fractional part is formatted to a fixed 2 decimal places first, since ".3" and
/// ".30" are otherwise indistinguishable by the time this runs -- the trailing zero, if
/// there ever was one, is already gone -- then those two digits are read directly as MM.
/// Only succeeds when that reads as a plausible minute value (< 60); the whole part
/// (hours) is *not* capped at < 24, matching parse_bare_time_string's own reasoning --
/// a "time" column is just as often a duration (elapsed hours) as a time-of-day, and
/// durations routinely exceed 23 hours. Negative values are rejected (not a time or a
/// duration either way), and the hour is read as a `u8`, so it naturally caps at 255
/// rather than growing unbounded. A genuine decimal quantity like a price or percentage
/// (e.g. 12.75) is still correctly left alone by the minutes check. HH:MM only,
/// deliberately -- there's no way to recover seconds from a single decimal fraction.
fn decimal_to_hm_string(value: f64) -> Option<String> {
    if value < 0.0 {
        return None;
    }
    let hours = value.trunc() as u8;
    let frac_digits = format!("{:.2}", value.fract().abs());
    let minutes: u32 = frac_digits.split('.').nth(1)?.parse().ok()?;
    if minutes >= 60 {
        return None;
    }
    Some(format!("{:02}:{:02}", hours, minutes))
}

fn process_string_value(value: &str, format: Format, def_val: Option<Value>) -> Value {
    match format {
        Format::Boolean => process_truthy_value(value, def_val, |v, ef| v.is_truthy_core(ef)),
        Format::Truthy => process_truthy_value(value, def_val, |v, ef| v.is_truthy_standard(ef)),
        Format::TruthyCustom(opts) => process_truthy_value(value, def_val, |v, _| {
            v.is_truthy_custom(&opts)
        }),
        Format::Decimal(places) => {
            process_numeric_value(value, def_val, |n| float_value(n.round_decimal(places)))
        }
        Format::Float => process_numeric_value(value, def_val, float_value),
        Format::Date => process_date_value(value, def_val, guess_date_string),
        Format::DateTime => process_date_value(value, def_val, guess_datetime_string),
        Format::DateTimeSimple => process_date_value(value, def_val, |s| {
            guess_datetime_string(s).map(|full| simplify_datetime_string(&full))
        }),
        Format::Time => process_date_value(value, def_val, |s| {
            guess_datetime_string(s)
                .and_then(|full| extract_time_portion(&full, false))
                .or_else(|| parse_bare_time_string(s))
        }),
        Format::Hm => process_date_value(value, def_val, |s| {
            guess_datetime_string(s)
                .and_then(|full| extract_time_portion(&full, true))
                .or_else(|| parse_bare_time_string(s))
        }),
        _ => Value::String(value.to_owned()),
    }
}

fn process_truthy_value<F>(value: &str, def_val: Option<Value>, truthy_fn: F) -> Value
where
    F: Fn(&str, bool) -> Option<bool>,
{
    if let Some(is_true) = truthy_fn(value, false) {
        Value::Bool(is_true)
    } else {
        def_val.unwrap_or(Value::Null)
    }
}

fn process_numeric_value<F>(value: &str, def_val: Option<Value>, numeric_fn: F) -> Value
where
    F: Fn(f64) -> Value,
{
    if let Some(n) = value.to_first_number::<f64>() {
        numeric_fn(n)
    } else {
        def_val.unwrap_or(Value::Null)
    }
}

fn process_date_value<F>(value: &str, def_val: Option<Value>, date_fn: F) -> Value
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(date_str) = date_fn(value) {
        string_value(&date_str)
    } else {
        def_val.unwrap_or(Value::Null)
    }
}

// Convert csv rows to value
fn csv_row_result_to_values(
    result: Result<StringRecord, csv::Error>,
    opts: &RowOptionSet,
) -> Option<Vec<Value>> {
    if let Ok(record) = result {
        let row = record
            .into_iter()
            .enumerate()
            .map(|(ci, cell)| csv_cell_to_json_value(cell, opts, ci))
            .collect();
        return Some(row);
    }
    None
}

// convert CSV cell &str value to a polymorphic serde_json::VALUE
fn csv_cell_to_json_value(cell: &str, opts: &RowOptionSet, index: usize) -> Value {
    // clean cell to check if it's numeric
    let col = opts.column(index);
    let (fmt, euro_num_mode) = if let Some(c) = col {
        (c.format.clone(), c.decimal_comma)
    } else {
        (Format::Auto, opts.decimal_comma)
    };
    // A column's own Format::Date/Format::Time/Format::Hm/Format::DateTime/
    // Format::DateTimeSimple override is checked before any numeric parsing, since a
    // date-like string ("2023-06-15") can
    // otherwise be misread as starting with a plain number ("2023") and fall through to
    // Value::Number instead of going through fuzzy-datetime at all.
    let def_val = col.and_then(|c| c.default.clone());
    match fmt {
        Format::Date => return process_date_value(cell, def_val, guess_date_string),
        Format::DateTime => return process_date_value(cell, def_val, guess_datetime_string),
        Format::DateTimeSimple => {
            return process_date_value(cell, def_val, |s| {
                guess_datetime_string(s).map(|full| simplify_datetime_string(&full))
            })
        }
        Format::Time => {
            return process_date_value(cell, def_val, |s| {
                guess_datetime_string(s)
                    .and_then(|full| extract_time_portion(&full, false))
                    .or_else(|| parse_bare_time_string(s))
            })
        }
        Format::Hm => {
            return process_date_value(cell, def_val, |s| {
                guess_datetime_string(s)
                    .and_then(|full| extract_time_portion(&full, true))
                    .or_else(|| parse_bare_time_string(s))
            })
        }
        _ => {}
    }
    let has_number = cell.to_first_number::<f64>().is_some();
    let num_cell = if has_number {
        let euro_num_mode = uses_decimal_comma(cell, euro_num_mode);
        if euro_num_mode {
            cell.replace(",", ".").replace(",", ".")
        } else {
            cell.replace(",", "")
        }
    } else {
        cell.to_owned()
    };
    let mut new_cell = Value::Null;
    if !num_cell.is_empty() && num_cell.is_numeric() {
        if let Ok(float_val) = serde_json::Number::from_str(&num_cell) {
            match fmt {
                Format::Integer => {
                    // as_i128() only succeeds for Numbers that are already integer-valued
                    // internally, so it silently yields 0 for any decimal value (e.g. "58.2")
                    // via unwrap_or(0). Go through as_f64() and truncate instead, matching
                    // the equivalent xlsx/ods cell conversion in process_float_value above.
                    if let Some(f) = float_val.as_f64() {
                        if let Some(int_val) = Number::from_i128(f as i128) {
                            new_cell = Value::Number(int_val);
                        }
                    }
                }
                Format::Boolean => {
                    // only 1.0 or more will evaluate as true
                    new_cell = Value::Bool(float_val.as_f64().unwrap_or(0f64) >= 1.0);
                }
                Format::Decimal(places) => {
                    if let Some(f) = float_val.as_f64() {
                        new_cell = float_value(f.round_decimal(places));
                    }
                }
                _ => {
                    new_cell = Value::Number(float_val);
                }
            }
        }
    } else if let Some(is_true) = cell.is_truthy_core(false) {
        new_cell = Value::Bool(is_true);
    } else {
        new_cell = match fmt {
            Format::Truthy => {
                if let Some(is_true) = cell.is_truthy_standard(false) {
                    Value::Bool(is_true)
                } else {
                    Value::Null
                }
            }
            _ => Value::String(cell.to_string()),
        };
    }
    new_cell
}

pub async fn read_workbook_sheet_info<'a>(
    path_data: &PathData<'a>,
) -> Result<IndexMap<String, usize>, GenericError> {
    if let Ok(mut workbook) = open_workbook_auto(path_data.path()) {
        let mut im: IndexMap<String, usize> = IndexMap::new();
        for name in workbook.sheet_names() {
            if let Ok(range) = workbook.worksheet_range(&name) {
                im.insert(name, range.rows().count());
            }
        }
        Ok(im)
    } else {
        Err(GenericError("cannot_open_workbook"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{helpers::*, Column};
    use serde_json::json;
    use std::path;

    /// Generates a workbook laid out the way many real-world spreadsheets are: a title
    /// row, a notes row, the real header row, a blank gap row, then the actual data --
    /// for testing header_row/data_row_index against a realistic gap scenario. Row 0
    /// (0-based) "Report Title", row 1 "Generated 2026-01-01", row 2 header ("sku",
    /// "qty"), row 3 blank, rows 4-5 data (SKU001/10, SKU002/20).
    fn gen_header_gap_fixture(filename: &str) -> String {
        use rust_xlsxwriter::Workbook;
        let mut workbook = Workbook::new();
        let sheet = workbook.add_worksheet().set_name("Sheet1").unwrap();
        sheet.write_string(0, 0, "Report Title").unwrap();
        sheet.write_string(1, 0, "Generated 2026-01-01").unwrap();
        sheet.write_string(2, 0, "sku").unwrap();
        sheet.write_string(2, 1, "qty").unwrap();
        // row 3 intentionally left blank
        sheet.write_string(4, 0, "SKU001").unwrap();
        sheet.write_number(4, 1, 10.0).unwrap();
        sheet.write_string(5, 0, "SKU002").unwrap();
        sheet.write_number(5, 1, 20.0).unwrap();
        let path = std::env::temp_dir().join(filename);
        workbook.save(&path).unwrap();
        path.to_string_lossy().to_string()
    }

    /// The user-supplied example that motivated auto-detection: a title row, a proper
    /// 3-column header, an explanatory-text row that only fills one cell, then real
    /// data. header_row=1, data_row_index=3 (both 0-based) is the expected guess.
    fn gen_auto_detect_fixture(filename: &str) -> String {
        use rust_xlsxwriter::Workbook;
        let mut workbook = Workbook::new();
        let sheet = workbook.add_worksheet().set_name("Sheet1").unwrap();
        sheet.write_string(0, 0, "Sales 2025").unwrap();
        sheet.write_string(1, 0, "region").unwrap();
        sheet.write_string(1, 1, "team size").unwrap();
        sheet.write_string(1, 2, "revenue").unwrap();
        sheet.write_string(2, 0, "long explanation about the data").unwrap();
        sheet.write_string(3, 0, "west").unwrap();
        sheet.write_number(3, 1, 12.0).unwrap();
        sheet.write_number(3, 2, 923456.0).unwrap();
        sheet.write_string(4, 0, "east").unwrap();
        sheet.write_number(4, 1, 7.0).unwrap();
        sheet.write_number(4, 2, 817285.0).unwrap();
        let path = std::env::temp_dir().join(filename);
        workbook.save(&path).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn test_detect_header_opt_in_finds_header_and_data_row_xlsx() {
        // detect_header is off by default for direct library use (see the field's own
        // doc) -- callers that want auto-detection opt in explicitly via .detect_header().
        let path = gen_auto_detect_fixture("auto_detect.xlsx");
        let opts = OptionSet::new(&path).detect_header();
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 2, "the explanation row must not leak through as data");
        assert_eq!(rows[0].get("region"), Some(&json!("west")));
        assert_eq!(rows[0].get("team_size"), Some(&json!(12.0)));
        assert_eq!(rows[0].get("revenue"), Some(&json!(923456.0)));
        assert_eq!(rows[1].get("region"), Some(&json!("east")));
    }

    #[test]
    fn test_detect_header_opt_in_finds_header_and_data_row_csv() {
        let path = write_csv_fixture(
            "auto_detect.csv",
            "Sales 2025\nregion,team size,revenue\nlong explanation about the data\nwest,12,923456\neast,7,817285\n",
        );
        let opts = OptionSet::new(&path).detect_header();
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 2, "the explanation row must not leak through as data");
        assert_eq!(rows[0].get("region"), Some(&json!("west")));
        assert_eq!(rows[0].get("team_size"), Some(&json!(12)));
        assert_eq!(rows[1].get("region"), Some(&json!("east")));
    }

    #[test]
    fn test_detect_header_falls_back_to_fallback_naming_for_headerless_text_csv() {
        // No header row exists at all, and there's no numeric/boolean/date signal
        // anywhere (a content-migration file) -- detection must not consume the first
        // row as a bogus header, losing it as data. Field names fall back to A1-style
        // letters, same as --omit-header, since there's no header text to derive from.
        let path = write_csv_fixture(
            "headerless_migration.csv",
            "welcome_msg,Welcome to our store,Bienvenue dans notre magasin\ngoodbye_msg,Thank you for visiting,Merci de votre visite\n",
        );
        let opts = OptionSet::new(&path).detect_header();
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 2, "both rows must be present, none consumed as a header");
        assert_eq!(rows[0].get("a"), Some(&json!("welcome_msg")));
        assert_eq!(rows[0].get("b"), Some(&json!("Welcome to our store")));
        assert_eq!(rows[1].get("a"), Some(&json!("goodbye_msg")));
    }

    #[test]
    fn test_detect_header_falls_back_to_fallback_naming_for_headerless_text_xlsx() {
        use rust_xlsxwriter::Workbook;
        let mut workbook = Workbook::new();
        let sheet = workbook.add_worksheet().set_name("Sheet1").unwrap();
        sheet.write_string(0, 0, "welcome_msg").unwrap();
        sheet.write_string(0, 1, "Welcome to our store").unwrap();
        sheet.write_string(0, 2, "Bienvenue dans notre magasin").unwrap();
        sheet.write_string(1, 0, "goodbye_msg").unwrap();
        sheet.write_string(1, 1, "Thank you for visiting").unwrap();
        sheet.write_string(1, 2, "Merci de votre visite").unwrap();
        let path = std::env::temp_dir().join("headerless_migration.xlsx");
        workbook.save(&path).unwrap();

        let opts = OptionSet::new(path.to_str().unwrap()).detect_header();
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 2, "both rows must be present, none consumed as a header");
        assert_eq!(rows[0].get("a"), Some(&json!("welcome_msg")));
        assert_eq!(rows[1].get("a"), Some(&json!("goodbye_msg")));
    }

    #[test]
    fn test_omit_header_uses_fallback_names_for_xlsx_not_real_header_text() {
        // Regression: --omit-header used to be a no-op for xlsx/ods -- real header text
        // was still used for field names regardless of the flag, because the row-0
        // shortcut (range.headers()) ran unconditionally. Now gated on capture_headers,
        // shared with the detect_header fallback-naming path above.
        let sample_path = "data/sample-data-1.xlsx";
        let opts = OptionSet::new(sample_path).omit_header();
        let result = process_spreadsheet_direct(&opts).unwrap();
        assert!(result.keys.contains(&"a".to_string()), "got: {:?}", result.keys);
        assert!(!result.keys.contains(&"id".to_string()), "got: {:?}", result.keys);
        let rows = result.to_vec();
        // the literal header row's own text is now the first "data" row, since no row
        // is consumed for headers at all
        assert_eq!(rows[0].get("a"), Some(&json!("id")));
    }

    #[test]
    fn test_detect_header_off_by_default_leaves_notes_rows_uncleaned() {
        // Without .detect_header(), the library falls back to its old, simple default
        // (row 0 is the header) even on a file that has title/notes rows -- this is the
        // behavior direct library consumers should see unless they opt in.
        let path = gen_auto_detect_fixture("auto_detect_no_opt_in.xlsx");
        let opts = OptionSet::new(&path);
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        // row 0 ("Sales 2025") is treated as the header; every subsequent row (including
        // the real header and the notes row) is captured as data instead of being cleaned up
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn test_detect_header_does_not_change_output_for_a_normal_well_formed_file() {
        // Regression guard: with .detect_header() opted in, a file with no title/notes
        // rows at all (row 0 is already a perfectly good header) must produce identical
        // output to the plain default -- detection should just confirm row 0, not guess
        // something else.
        let sample_path = "data/sample-data-1.xlsx";
        let opts = OptionSet::new(sample_path).max_row_count(1_000).detect_header();
        let result = process_spreadsheet_direct(&opts).unwrap();
        assert_eq!(result.num_rows, 401);
        assert_eq!(result.to_vec()[0].get("first_name"), Some(&json!("Dulce")));
    }

    #[test]
    fn test_header_row_without_data_row_index_captures_the_gap_row_as_data() {
        // Baseline: with only header_row set (no data_row_index), the row directly
        // below the header is treated as data, even though it's actually a blank gap
        // row here -- this is the pre-existing behavior data_row_index exists to fix.
        let path = gen_header_gap_fixture("header_gap_baseline.xlsx");
        let opts = OptionSet::new(&path).header_row(2);
        let result = process_spreadsheet_direct(&opts).unwrap();
        let rows = result.to_vec();
        assert_eq!(rows.len(), 3, "gap row, SKU001, SKU002");
        assert_eq!(rows[0].get("sku"), Some(&Value::Null));
        assert_eq!(rows[1].get("sku"), Some(&json!("SKU001")));
        assert_eq!(rows[2].get("sku"), Some(&json!("SKU002")));
    }

    #[test]
    fn test_data_row_index_skips_the_gap_between_header_and_real_data() {
        // header_row=2 (the "sku"/"qty" row), data_row_index=4 (the SKU001 row), both
        // 0-based -- row 3, the blank gap row, is skipped entirely rather than captured
        // as a null row.
        let path = gen_header_gap_fixture("header_gap_with_data_row.xlsx");
        let opts = OptionSet::new(&path).header_row(2).data_row_index(4);
        let result = process_spreadsheet_direct(&opts).unwrap();
        let rows = result.to_vec();
        assert_eq!(rows.len(), 2, "just SKU001 and SKU002, no gap row");
        assert_eq!(rows[0].get("sku"), Some(&json!("SKU001")));
        assert_eq!(rows[0].get("qty"), Some(&json!(10.0)));
        assert_eq!(rows[1].get("sku"), Some(&json!("SKU002")));
        assert_eq!(rows[1].get("qty"), Some(&json!(20.0)));
    }

    #[test]
    fn test_result_set_reports_the_resolved_header_and_body_start_indices() {
        // Regression: ResultSet.header_row_index/body_start_index used to not exist at
        // all -- callers had no way to learn what row indices a read actually used,
        // whether from an explicit override (this test) or auto-detection (the next
        // one), since OptionSet.header_row/.data_row_index only ever reflect an
        // explicit override and stay None otherwise.
        let path = gen_header_gap_fixture("header_gap_reports_resolved_indices.xlsx");
        let opts = OptionSet::new(&path).header_row(2).data_row_index(4);
        let result = process_spreadsheet_direct(&opts).unwrap();
        assert_eq!(result.header_row_index, Some(2));
        assert_eq!(result.body_start_index, 4);
    }

    #[test]
    fn test_result_set_reports_resolved_indices_from_auto_detection_too() {
        // Same as above, but with no explicit header_row/data_row_index at all -- the
        // resolved indices must reflect what auto-detection actually found, not just
        // echo back the (unset) explicit-override fields.
        let path = gen_header_gap_fixture("header_gap_reports_detected_indices.xlsx");
        let opts = OptionSet::new(&path).detect_header();
        let result = process_spreadsheet_direct(&opts).unwrap();
        assert_eq!(result.header_row_index, Some(2));
        assert_eq!(result.body_start_index, 4);
    }

    #[test]
    fn test_data_row_index_in_preview_multimode_skips_the_gap_too() {
        // Same fixture, read via --preview (multimode) -- the gap-skipping logic is
        // duplicated in read_multiple_worksheets, so it needs its own regression test.
        let path = gen_header_gap_fixture("header_gap_preview.xlsx");
        let opts = OptionSet::new(&path).header_row(2).data_row_index(4).read_mode_preview();
        let result = process_spreadsheet_direct(&opts).unwrap();
        let sheet_rows = result.data.first_sheet();
        assert_eq!(sheet_rows.len(), 2, "just SKU001 and SKU002, no gap row");
        assert_eq!(sheet_rows[0].get("sku"), Some(&json!("SKU001")));
        assert_eq!(sheet_rows[1].get("sku"), Some(&json!("SKU002")));
    }

    #[test]
    fn test_direct_processing_xlsx() {
        let sample_path = "data/sample-data-1.xlsx";

        // instantiate the OptionSet with a sample path and a maximum row count of 1000 rows as the source file has 401 rows
        // (although )the default max is 10,000)
        let opts = OptionSet::new(sample_path).max_row_count(1_000);

        let result = process_spreadsheet_direct(&opts);

        // The source file should have 1 header row and 400 data rows
        assert_eq!(result.unwrap().num_rows, 401);
    }

    #[test]
    fn test_source_key_override_renames_and_reformats_without_position() {
        // End-to-end: overriding just one field out of many by its natural key,
        // without needing to enumerate/pad the columns ahead of it.
        let sample_path = "data/sample-data-1.csv";
        let mut opts = OptionSet::new(sample_path).max_row_count(2);
        opts.rows.columns = vec![
            Column::from_source_key_with_format("weight", Some("weight_lbs"), Format::Integer, None, DateTimeMode::Full, false),
        ];

        let result = process_spreadsheet_direct(&opts).unwrap();
        // every other column keeps its natural, auto-detected name
        assert!(result.keys.contains(&"id".to_string()));
        assert!(result.keys.contains(&"first_name".to_string()));
        assert!(result.keys.contains(&"weight_lbs".to_string()));
        assert!(!result.keys.contains(&"weight".to_string()));

        let rows = result.to_vec();
        let first = rows.first().expect("at least one row");
        assert!(first.get("weight_lbs").is_some());
        assert!(first.get("weight").is_none());
        assert_eq!(first.get("id").unwrap(), 1);
    }

    #[test]
    fn test_resolve_datetime_mode_prefers_column_format_over_row_defaults() {
        // A column's own Format::Date/Format::Time/Format::DateTime overrides the
        // row-wide default; Format::Auto (and anything else) falls back to the column's
        // own datetime_mode next, then the row-wide default.
        assert_eq!(resolve_datetime_mode(&Format::Date, DateTimeMode::Full, DateTimeMode::Full), DateTimeMode::DateOnly);
        assert_eq!(resolve_datetime_mode(&Format::Time, DateTimeMode::Full, DateTimeMode::Full), DateTimeMode::TimeOnly);
        assert_eq!(resolve_datetime_mode(&Format::Hm, DateTimeMode::Full, DateTimeMode::Full), DateTimeMode::HmOnly);
        assert_eq!(resolve_datetime_mode(&Format::DateTimeSimple, DateTimeMode::Full, DateTimeMode::Full), DateTimeMode::Simple);
        assert_eq!(resolve_datetime_mode(&Format::DateTime, DateTimeMode::DateOnly, DateTimeMode::TimeOnly), DateTimeMode::Full);
        assert_eq!(resolve_datetime_mode(&Format::Auto, DateTimeMode::Full, DateTimeMode::Full), DateTimeMode::Full);
        assert_eq!(resolve_datetime_mode(&Format::Auto, DateTimeMode::DateOnly, DateTimeMode::Full), DateTimeMode::DateOnly);
        assert_eq!(resolve_datetime_mode(&Format::Auto, DateTimeMode::Full, DateTimeMode::TimeOnly), DateTimeMode::TimeOnly);
        assert_eq!(resolve_datetime_mode(&Format::Auto, DateTimeMode::HmOnly, DateTimeMode::DateOnly), DateTimeMode::HmOnly);
    }

    #[test]
    fn test_source_key_override_casts_native_datetime_cell_to_date_only() {
        // Regression test: workbook_cell_to_value computed the column's Format override
        // but only ever consulted the row-wide --date-only flag for Data::DateTime /
        // Data::DateTimeIso cells, so a per-column `Format::Date` override on a real
        // (non-string) datetime cell had no effect at all.
        let sample_path = "data/sample-data-1.xlsx";
        let mut opts = OptionSet::new(sample_path).max_row_count(1);
        opts.rows.columns = vec![
            Column::from_source_key_with_format("start_time", None, Format::Date, None, DateTimeMode::Full, false),
        ];

        let result = process_spreadsheet_direct(&opts).unwrap();
        let rows = result.to_vec();
        let first = rows.first().expect("at least one row");
        let start_time = first.get("start_time").expect("start_time column").as_str().unwrap();
        assert_eq!(start_time, "2023-06-15");
        assert!(!start_time.contains('T'), "should be date-only, got: {}", start_time);
    }

    #[test]
    fn test_time_only_excel_cell_does_not_carry_the_epoch_placeholder_date() {
        // Regression: Excel has no true time-only type -- a cell formatted as plain
        // "hh:mm" (e.g. a recurring daily start time like "6:30") is really a full
        // datetime serial with zero elapsed days, which calamine converts by landing on
        // its epoch ("1899-12-31" in the 1900 date system). Formatting the whole thing
        // as a datetime carried that meaningless placeholder date through to the output
        // ("1899-12-31T06:30:00.000Z"); it should come back as a bare time instead.
        use rust_xlsxwriter::{ExcelDateTime, Format as XlsxFormat, Workbook};

        let mut workbook = Workbook::new();
        let sheet = workbook.add_worksheet().set_name("Sheet1").unwrap();
        let time_fmt = XlsxFormat::new().set_num_format("hh:mm");
        sheet.write_string(0, 0, "meal").unwrap();
        sheet.write_string(0, 1, "start").unwrap();
        sheet.write_string(1, 0, "Breakfast").unwrap();
        let breakfast_time = ExcelDateTime::from_hms(6, 30, 0).unwrap();
        sheet.write_time_with_format(1, 1, breakfast_time, &time_fmt).unwrap();
        // a genuine full date+time, for contrast -- must still include the real date
        sheet.write_string(2, 0, "Meeting").unwrap();
        let meeting_dt = ExcelDateTime::from_ymd(2026, 3, 5).unwrap().and_hms(9, 0, 0).unwrap();
        let datetime_fmt = XlsxFormat::new().set_num_format("yyyy-mm-dd hh:mm");
        sheet.write_datetime_with_format(2, 1, meeting_dt, &datetime_fmt).unwrap();
        let path = std::env::temp_dir().join("time_only_cell.xlsx");
        workbook.save(&path).unwrap();

        let opts = OptionSet::new(path.to_str().unwrap());
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows[0].get("start"), Some(&json!("06:30:00")));
        assert_eq!(rows[1].get("start"), Some(&json!("2026-03-05T09:00:00.000Z")));
    }

    #[test]
    fn test_format_time_column_override_forces_time_only_even_for_a_full_datetime() {
        // Format::Time is an explicit override, distinct from the automatic
        // as_f64() < 1.0 detection above -- it must strip the date component even from
        // a cell that carries a genuine, non-epoch date, e.g. a per-column override on
        // a "logged_at" timestamp column where only the time-of-day is wanted.
        use rust_xlsxwriter::{ExcelDateTime, Format as XlsxFormat, Workbook};

        let mut workbook = Workbook::new();
        let sheet = workbook.add_worksheet().set_name("Sheet1").unwrap();
        sheet.write_string(0, 0, "logged_at").unwrap();
        let dt = ExcelDateTime::from_ymd(2026, 3, 5).unwrap().and_hms(9, 15, 30).unwrap();
        let datetime_fmt = XlsxFormat::new().set_num_format("yyyy-mm-dd hh:mm:ss");
        sheet.write_datetime_with_format(1, 0, dt, &datetime_fmt).unwrap();
        let path = std::env::temp_dir().join("format_time_override_cell.xlsx");
        workbook.save(&path).unwrap();

        let mut opts = OptionSet::new(path.to_str().unwrap());
        opts.rows.columns = vec![
            Column::from_source_key_with_format("logged_at", None, Format::Time, None, DateTimeMode::Full, false),
        ];
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows[0].get("logged_at"), Some(&json!("09:15:30")));
    }

    #[test]
    fn test_row_wide_time_only_strips_the_date_from_a_native_iso_datetime_cell() {
        // opts.rows.datetime_mode mirrors --date-only but for the opposite end: with no
        // per-column override, it should reduce a full ISO datetime cell down to just
        // "HH:MM:SS.mmm" -- post-processing fuzzy-datetime's output, not a change to
        // the fuzzy-datetime crate itself (out of scope).
        let row_opts = RowOptionSet { datetime_mode: DateTimeMode::TimeOnly, ..Default::default() };
        let cell = Data::DateTimeIso("2023-06-15T10:17:00.000Z".to_string());
        assert_eq!(
            workbook_cell_to_value(&cell, &row_opts, 0),
            Value::String("10:17:00".to_string())
        );
    }

    #[test]
    fn test_row_wide_hm_only_truncates_seconds_from_a_native_datetime_cell() {
        // --hm-only is coarser than --time-only: a start/end time or recurring daily
        // slot is usually better read as "09:15" than "09:15:30.000".
        let row_opts = RowOptionSet { datetime_mode: DateTimeMode::HmOnly, ..Default::default() };
        let cell = Data::DateTimeIso("2023-06-15T09:15:30.000Z".to_string());
        assert_eq!(
            workbook_cell_to_value(&cell, &row_opts, 0),
            Value::String("09:15".to_string())
        );
    }

    #[test]
    fn test_csv_cell_format_time_extracts_time_of_day_from_a_plain_date_string() {
        // csv_cell_to_json_value previously had no Date/DateTime/Time handling at all --
        // a date-like string starting with a number (e.g. "2023-06-15T10:17") could be
        // misread by the numeric-extraction path before ever reaching fuzzy-datetime.
        let cols = vec![Column::new_format(Format::Time, None)];
        let row_opts = RowOptionSet::simple(&cols);
        assert_eq!(
            csv_cell_to_json_value("2023-06-15T10:17:00", &row_opts, 0),
            Value::String("10:17:00".to_string())
        );
    }

    #[test]
    fn test_csv_cell_format_hm_drops_seconds_too() {
        // Format::Hm ("|hm" in --keys) is the CSV/string-cell equivalent of --hm-only,
        // e.g. spread-cli --keys "served_from|hm" for a restaurant menu's serving times.
        let cols = vec![Column::new_format(Format::Hm, None)];
        let row_opts = RowOptionSet::simple(&cols);
        assert_eq!(
            csv_cell_to_json_value("2023-06-15T09:15:30", &row_opts, 0),
            Value::String("09:15".to_string())
        );
    }

    #[test]
    fn test_parse_bare_time_string_extracts_and_validates_via_to_numbers() {
        // to_numbers::<u8>() extracts the numeric components directly (no regex, no
        // manual splitting), so real minutes/seconds range validation -- and zero-padded
        // reformatting -- comes essentially for free; a superset check ("starts/ends
        // with a digit") would have let a clearly-implausible value like "11:60" through
        // as a bogus "valid" time.
        assert_eq!(parse_bare_time_string("11:39"), Some("11:39".to_string()));
        // reformatted with zero-padding, not passed through as typed
        assert_eq!(parse_bare_time_string("9:5:3"), Some("09:05:03".to_string()));
        assert_eq!(parse_bare_time_string(" 23:59:59 "), Some("23:59:59".to_string()));
        // '.' works exactly the same as ':' -- the dot-typed-time case
        assert_eq!(parse_bare_time_string("12.30"), Some("12:30".to_string()));
        // hours are deliberately NOT capped at < 24 -- a "time" column is just as often
        // a duration (elapsed hours: a race, a video, a timesheet total) as a
        // time-of-day, and durations routinely exceed 23 hours
        assert_eq!(parse_bare_time_string("27:45:00"), Some("27:45:00".to_string()));
        assert_eq!(parse_bare_time_string("100:00"), Some("100:00".to_string()));
        // minutes/seconds are always bounded to < 60 regardless -- out of range here is
        // correctly rejected, not passed through as a bogus result
        assert_eq!(parse_bare_time_string("11:60"), None);
        // clearly not a time at all -- no digits, or not exactly 2-3 numeric parts
        assert_eq!(parse_bare_time_string("not a time"), None);
        assert_eq!(parse_bare_time_string("12"), None);
        // a genuine decimal number that also happens to read as a plausible hh:mm is an
        // inherent, accepted ambiguity of the dot-as-colon convention (same one behind
        // "4.5" as 4h30m in decimal-hours notation vs. 4:50) -- Format::Time/Format::Hm
        // is an explicit "I know this is a time" assertion, so this isn't second-guessed
        assert_eq!(parse_bare_time_string("3.14"), Some("03:14".to_string()));
    }

    #[test]
    fn test_parse_bare_time_string_converts_a_trailing_am_pm_marker_to_24_hour() {
        // A trailing am/pm marker is detected and converted to 24-hour notation, whether
        // or not it's separated from the digits by a space, and regardless of case.
        assert_eq!(parse_bare_time_string("2:30 PM"), Some("14:30".to_string()));
        assert_eq!(parse_bare_time_string("2:30PM"), Some("14:30".to_string()));
        assert_eq!(parse_bare_time_string("2:30pm"), Some("14:30".to_string()));
        // 12-hour-clock oddities: 12am is midnight, 12pm (noon) is unchanged
        assert_eq!(parse_bare_time_string("12:45am"), Some("00:45".to_string()));
        assert_eq!(parse_bare_time_string("12:00 PM"), Some("12:00".to_string()));
        assert_eq!(parse_bare_time_string("11:39 AM"), Some("11:39".to_string()));
        // also applies to the 3-part hh:mm:ss shape
        assert_eq!(parse_bare_time_string("11:39:05 PM"), Some("23:39:05".to_string()));
        // an hour outside 1-12 alongside an am/pm marker isn't genuine 12-hour notation
        // at all -- the 12-hour clock never produces those hours -- so it's trusted as
        // already-literal 24-hour and the redundant suffix is dropped, not rejected
        assert_eq!(parse_bare_time_string("14:00pm"), Some("14:00".to_string()));
        assert_eq!(parse_bare_time_string("14:30pm"), Some("14:30".to_string()));
        assert_eq!(parse_bare_time_string("0:30am"), Some("00:30".to_string()));
    }

    #[test]
    fn test_parse_bare_time_string_rejects_anything_else_with_letters_rather_than_guess() {
        // Regression: to_numbers::<u8>() silently discards non-digit characters, so
        // without an explicit guard "2h45m" would extract as [2, 45] and format as
        // "02:45" -- not a rejection, a *wrong* answer indistinguishable from a correct
        // one. Duration-unit notation is a different, out-of-scope notation -- rejected,
        // not silently (and wrongly) reinterpreted as hh:mm.
        assert_eq!(parse_bare_time_string("2h45m"), None);
        assert_eq!(parse_bare_time_string("not a time"), None);
    }

    #[test]
    fn test_format_time_falls_back_to_a_bare_time_string_with_no_date_component() {
        // Regression: a cell already containing just a time, e.g. "11:39" (no date at
        // all, common for a spreadsheet column pre-formatted as text), returned null
        // under Format::Time/Format::Hm -- iso_fuzzy_to_datetime_string requires a date
        // component to parse anything at all, so it always failed on a bare time, and
        // the whole thing fell through to the column's default (null, absent one).
        // Format::Time/Hm should treat an already-bare time string as already correct,
        // not demand a date it was never asked to interpret -- passed through as-is,
        // matching parse_bare_time_string's own loose, non-reformatting behavior.
        let time_cols = vec![Column::new_format(Format::Time, None)];
        let time_opts = RowOptionSet::simple(&time_cols);
        assert_eq!(csv_cell_to_json_value("11:39", &time_opts, 0), Value::String("11:39".to_string()));

        let hm_cols = vec![Column::new_format(Format::Hm, None)];
        let hm_opts = RowOptionSet::simple(&hm_cols);
        assert_eq!(csv_cell_to_json_value("11:39", &hm_opts, 0), Value::String("11:39".to_string()));

        // a genuine full datetime string still goes through the original extraction path,
        // not the bare-time fallback -- unaffected by this change
        assert_eq!(
            csv_cell_to_json_value("2023-06-15T09:15:30", &time_opts, 0),
            Value::String("09:15:30".to_string())
        );

        // the xlsx/ods string-cell path (process_string_value) shares the same fix
        assert_eq!(process_string_value("11:39", Format::Time, None), Value::String("11:39".to_string()));
        assert_eq!(process_string_value("11:39", Format::Hm, None), Value::String("11:39".to_string()));
    }

    #[test]
    fn test_decimal_to_hm_string_reads_the_fraction_as_literal_minutes_not_a_scaled_fraction() {
        // "12.30" typed by a user (meaning 12:30) commonly arrives here as the bare float
        // 12.3 -- Excel/Sheets have no time type for a dot-typed value, so it's silently
        // read as a decimal number, trailing zero dropped. The fractional digits must be
        // read literally as MM (0.3 -> "30", i.e. the trailing zero restored), never
        // scaled by 60 (0.3 of an hour would be 18 minutes, not 30).
        assert_eq!(decimal_to_hm_string(12.3), Some("12:30".to_string()));
        assert_eq!(decimal_to_hm_string(9.05), Some("09:05".to_string()));
        assert_eq!(decimal_to_hm_string(0.0), Some("00:00".to_string()));
        assert_eq!(decimal_to_hm_string(23.59), Some("23:59".to_string()));
        // hours are NOT capped at < 24 -- a "time" column is just as often a duration
        // (elapsed hours) as a time-of-day, and durations routinely exceed 23 hours;
        // there's no way to tell which from the value alone, so this doesn't guess
        assert_eq!(decimal_to_hm_string(24.0), Some("24:00".to_string()));
        assert_eq!(decimal_to_hm_string(100.3), Some("100:30".to_string()));
        // a genuine decimal quantity (price, percentage, ...) must still be left alone --
        // >= 60 "minutes" isn't a plausible time under any interpretation, and a negative
        // value is neither a time-of-day nor a duration
        assert_eq!(decimal_to_hm_string(12.75), None);
        assert_eq!(decimal_to_hm_string(-1.0), None);
    }

    #[test]
    fn test_format_time_reconstructs_hm_from_a_decimal_disguised_time() {
        // The CSV/text-cell path: "12.30" survives as literal text only when a cell is
        // explicitly formatted/typed as text (bypassing Excel's auto-decimal-conversion);
        // otherwise it's already a float by the time it reaches spreadsheet_to_json at
        // all -- covered by the native Data::Float case below.
        //
        // Strings don't have the float case's trailing-zero-loss problem (a string
        // preserves exactly what was typed), so this just swaps '.' for ':' and reuses
        // parse_bare_time_string's own to_numbers-based extraction and validation --
        // not decimal_to_hm_string's reconstruction, which the native-float case still
        // needs. Single-digit components are zero-padded on the way out ("12.3" ->
        // "12:03"), same as parse_bare_time_string does for a literally-typed "12:3".
        let time_cols = vec![Column::new_format(Format::Time, None)];
        let time_opts = RowOptionSet::simple(&time_cols);
        assert_eq!(csv_cell_to_json_value("12.30", &time_opts, 0), Value::String("12:30".to_string()));
        assert_eq!(csv_cell_to_json_value("12.3", &time_opts, 0), Value::String("12:03".to_string()));

        let hm_cols = vec![Column::new_format(Format::Hm, None)];
        let hm_opts = RowOptionSet::simple(&hm_cols);
        assert_eq!(csv_cell_to_json_value("12.30", &hm_opts, 0), Value::String("12:30".to_string()));

        // the xlsx/ods text-cell path shares the same fallback
        assert_eq!(process_string_value("12.30", Format::Time, None), Value::String("12:30".to_string()));

        // range validation now applies to the string path too (parse_bare_time_string's
        // own to_numbers-based check), so a genuinely implausible "time" is correctly
        // rejected rather than passed through as a bogus result
        assert_eq!(csv_cell_to_json_value("12.75", &time_opts, 0), Value::Null);

        // the native Data::Float case -- the far more common real-world path, since
        // Excel/Sheets normally convert a dot-typed time entry to a float outright
        // rather than leaving it as text. Unlike the string case, the trailing zero is
        // already gone by the time this runs (12.30 and 12.3 are the same f64), so this
        // one *does* need decimal_to_hm_string's separate numeric reconstruction.
        assert_eq!(process_float_value(12.3, Format::Time, None), Value::String("12:30".to_string()));
        assert_eq!(process_float_value(12.3, Format::Hm, None), Value::String("12:30".to_string()));

        // a genuine price/decimal column with Format::Time forced on it (user error, or
        // just not a time column) correctly yields null rather than a bogus time
        assert_eq!(process_float_value(12.75, Format::Time, None), Value::Null);
    }

    #[test]
    fn test_format_date_recognizes_slash_separated_dates_of_any_order() {
        // Regression: Format::Date/DateTime/... on a text cell or CSV value used
        // iso_fuzzy_to_date_string/iso_fuzzy_to_datetime_string, which force
        // DateOrder::YMD *and* '-' as the only accepted separator (DateOptions::default())
        // -- passing an explicit DateOptions skips fuzzy-datetime's own order/separator
        // guessing entirely. Any '/'-separated date -- by far the most common separator
        // worldwide, in any of DMY/MDY/YMD order -- returned null outright, even the
        // unambiguous "2026/07/19" (right order, just the "wrong" separator). Now uses
        // fuzzy-datetime's guessing entry points instead, output shape unchanged.
        let cols = vec![Column::new_format(Format::Date, None)];
        let row_opts = RowOptionSet::simple(&cols);
        for (value, expected) in [
            ("19/07/2026", "2026-07-19"), // DMY (day 19 rules out MDY)
            ("07/19/2026", "2026-07-19"), // MDY (day 19 rules out DMY)
            ("2026/07/19", "2026-07-19"), // YMD, just not '-'
            ("2026-07-19", "2026-07-19"), // the already-working case, unaffected
        ] {
            assert_eq!(
                csv_cell_to_json_value(value, &row_opts, 0),
                Value::String(expected.to_string()),
                "{:?} should resolve to {:?}",
                value,
                expected
            );
        }
    }

    #[test]
    fn test_format_datetime_recognizes_slash_separated_dates_too() {
        let cols = vec![Column::new_format(Format::DateTime, None)];
        let row_opts = RowOptionSet::simple(&cols);
        assert_eq!(
            csv_cell_to_json_value("19/07/2026", &row_opts, 0),
            Value::String("2026-07-19T00:00:00.000Z".to_string())
        );
        // the xlsx/ods string-cell path shares the same fix
        assert_eq!(
            process_string_value("19/07/2026", Format::Date, None),
            Value::String("2026-07-19".to_string())
        );
    }

    #[test]
    fn test_format_date_recognizes_dot_separated_dates_too() {
        // Depends on the fuzzy-datetime 0.1.4 fix for segment_is_subseconds
        // misreading a bare 4-digit year as milliseconds-plus-timezone-suffix.
        let cols = vec![Column::new_format(Format::Date, None)];
        let row_opts = RowOptionSet::simple(&cols);
        assert_eq!(
            csv_cell_to_json_value("19.07.2026", &row_opts, 0),
            Value::String("2026-07-19".to_string())
        );
    }

    #[test]
    fn test_simplify_datetime_string_drops_milliseconds_and_trailing_z() {
        assert_eq!(simplify_datetime_string("2026-07-18T18:07:34.000Z"), "2026-07-18T18:07:34");
        // also tolerates a string with no fractional seconds or Z at all
        assert_eq!(simplify_datetime_string("2026-07-18T18:07:34"), "2026-07-18T18:07:34");
    }

    #[test]
    fn test_row_wide_simple_mode_strips_milliseconds_and_z_from_a_native_iso_datetime_cell() {
        let row_opts = RowOptionSet { datetime_mode: DateTimeMode::Simple, ..Default::default() };
        let cell = Data::DateTimeIso("2026-07-18T18:07:34.000Z".to_string());
        assert_eq!(
            workbook_cell_to_value(&cell, &row_opts, 0),
            Value::String("2026-07-18T18:07:34".to_string())
        );
    }

    #[test]
    fn test_csv_cell_format_datetime_simple_drops_milliseconds_and_z() {
        // Format::DateTimeSimple ("|simple" or "|ds" in --keys) is the CSV/string-cell
        // equivalent of --simple: the full datetime, minus the JS-interop-oriented
        // milliseconds/trailing-Z formatting used by the default Full mode.
        let cols = vec![Column::new_format(Format::DateTimeSimple, None)];
        let row_opts = RowOptionSet::simple(&cols);
        assert_eq!(
            csv_cell_to_json_value("2026-07-18T18:07:34", &row_opts, 0),
            Value::String("2026-07-18T18:07:34".to_string())
        );
    }

    #[test]
    fn test_simple_mode_still_avoids_the_epoch_placeholder_date_for_a_genuine_time_only_excel_cell() {
        // Simple is still "the whole datetime", just reformatted -- so it must keep the
        // same auto time-only detection as Full mode (see
        // test_time_only_excel_cell_does_not_carry_the_epoch_placeholder_date), just
        // without milliseconds this time.
        use rust_xlsxwriter::{ExcelDateTime, Format as XlsxFormat, Workbook};

        let mut workbook = Workbook::new();
        let sheet = workbook.add_worksheet().set_name("Sheet1").unwrap();
        let time_fmt = XlsxFormat::new().set_num_format("hh:mm");
        sheet.write_string(0, 0, "start").unwrap();
        let breakfast_time = ExcelDateTime::from_hms(6, 30, 0).unwrap();
        sheet.write_time_with_format(1, 0, breakfast_time, &time_fmt).unwrap();
        let path = std::env::temp_dir().join("simple_mode_time_only_cell.xlsx");
        workbook.save(&path).unwrap();

        let mut opts = OptionSet::new(path.to_str().unwrap());
        opts.rows.datetime_mode = DateTimeMode::Simple;
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows[0].get("start"), Some(&json!("06:30:00")));
    }

    #[test]
    fn test_column_datetime_mode_applies_only_to_genuine_datetime_cells_under_format_auto() {
        // Column::datetime_mode (distinct from Format::Date/Time/Hm/DateTime) is scoped
        // to columns left at Format::Auto -- it only ever touches a cell that's already
        // a genuine datetime (Data::DateTime/Data::DateTimeIso), leaving strings and
        // numbers in the same column completely untouched, unlike an explicit Format
        // override which would force-interpret every cell type.
        let cols = vec![Column::from_key_ref_with_format(None, Format::Auto, None, DateTimeMode::HmOnly, false)];
        let row_opts = RowOptionSet::simple(&cols);
        let datetime_cell = Data::DateTimeIso("2023-06-15T09:15:30.000Z".to_string());
        assert_eq!(
            workbook_cell_to_value(&datetime_cell, &row_opts, 0),
            Value::String("09:15".to_string())
        );
        let string_cell = Data::String("not a date".to_string());
        assert_eq!(
            workbook_cell_to_value(&string_cell, &row_opts, 0),
            Value::String("not a date".to_string())
        );
    }

    #[test]
    fn test_native_float_cell_decimal_format_rounds_to_the_given_precision() {
        // Regression: Format::Decimal(places) had no arm at all in process_float_value --
        // it fell through to the plain Value::Number catch-all, so e.g. --keys "price|d2"
        // on a genuine (non-string) xlsx/ods float cell had no effect whatsoever.
        let cols = vec![Column::new_format(Format::Decimal(2), None)];
        let row_opts = RowOptionSet::simple(&cols);
        let cell = Data::Float(19.98765);
        assert_eq!(workbook_cell_to_value(&cell, &row_opts, 0), json!(19.99));
    }

    #[test]
    fn test_csv_cell_integer_format_truncates_decimal_values() {
        // Regression test: Number::as_i128() only succeeds for already-integer-valued
        // Numbers, so casting a decimal CSV cell like "58.2" to Format::Integer used to
        // silently produce 0 via unwrap_or(0) instead of the truncated value.
        let cols = vec![Column::new_format(Format::Integer, None)];
        let row_opts = RowOptionSet::simple(&cols);
        assert_eq!(csv_cell_to_json_value("58.2", &row_opts, 0), Value::Number(Number::from(58)));
        assert_eq!(csv_cell_to_json_value("82.5", &row_opts, 0), Value::Number(Number::from(82)));
        assert_eq!(csv_cell_to_json_value("100", &row_opts, 0), Value::Number(Number::from(100)));
    }

    #[test]
    fn test_csv_cell_decimal_format_rounds_to_the_given_precision() {
        // Regression: Format::Decimal(places) had no arm at all in csv_cell_to_json_value's
        // numeric match -- it fell through to the plain Value::Number catch-all, so
        // e.g. --keys "price|d2" on a CSV cell had no effect whatsoever.
        let cols = vec![Column::new_format(Format::Decimal(2), None)];
        let row_opts = RowOptionSet::simple(&cols);
        assert_eq!(csv_cell_to_json_value("19.98765", &row_opts, 0), json!(19.99));
        assert_eq!(csv_cell_to_json_value("5.4", &row_opts, 0), json!(5.4));
        assert_eq!(csv_cell_to_json_value("100", &row_opts, 0), json!(100.0));
    }

    #[test]
    fn test_csv_cell_does_not_coerce_ids_to_booleans() {
        // Regression test: these previously became `true`/`false` because their
        // embedded digit run (e.g. "SKU001" -> "001" -> 1) was fuzzily extracted
        // and matched against is_truthy_core's numeric range, even though the
        // column has no boolean intent (Format::Auto, the default).
        let row_opts = RowOptionSet::default();
        assert_eq!(csv_cell_to_json_value("SKU001", &row_opts, 0), Value::String("SKU001".to_string()));
        assert_eq!(csv_cell_to_json_value("A1", &row_opts, 0), Value::String("A1".to_string()));
        assert_eq!(csv_cell_to_json_value("01/06/2024", &row_opts, 0), Value::String("01/06/2024".to_string()));
        // literal boolean tokens should still be recognised
        assert_eq!(csv_cell_to_json_value("true", &row_opts, 0), Value::Bool(true));
        assert_eq!(csv_cell_to_json_value("false", &row_opts, 0), Value::Bool(false));
    }

    #[test]
    fn test_direct_processing_csv() {
        let sample_path = "data/sample-data-1.csv";

        // instantiate the OptionSet with a sample path and a maximum row count of 1000 rows as the source file has 401 rows
        // (although )the default max is 10,000)
        let opts = OptionSet::new(sample_path).max_row_count(1_000);

        let result = process_spreadsheet_direct(&opts);

        // The source file should have 1 header row and 400 data rows
        assert_eq!(result.unwrap().num_rows, 401);
    }

    /// Writes raw CSV text to a temp file for testing header_row/data_row_index/
    /// omit_header against CSV specifically (calamine fixtures need a real xlsx writer,
    /// but CSV is plain text -- no generator needed).
    fn write_csv_fixture(filename: &str, content: &str) -> String {
        let path = std::env::temp_dir().join(filename);
        std::fs::write(&path, content).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn test_csv_header_row_and_data_row_index_skip_a_gap() {
        // Row 0 title, row 1 notes, row 2 header, row 3 blank, rows 4-5 data --
        // the same shape as the xlsx gap fixture, but for CSV.
        let path = write_csv_fixture(
            "csv_header_gap.csv",
            "Report Title\nGenerated 2026-01-01\nsku,qty\n,\nSKU001,10\nSKU002,20\n",
        );

        // baseline: header_row alone (no data_row_index) captures the blank gap row --
        // CSV has no native null, so an empty field comes through as "" rather than null
        let opts = OptionSet::new(&path).header_row(2);
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 3, "gap row, SKU001, SKU002");
        assert_eq!(rows[0].get("sku"), Some(&json!("")));

        // data_row_index skips the gap row entirely
        let opts = OptionSet::new(&path).header_row(2).data_row_index(4);
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 2, "just SKU001 and SKU002");
        assert_eq!(rows[0].get("sku"), Some(&json!("SKU001")));
        assert_eq!(rows[1].get("sku"), Some(&json!("SKU002")));
    }

    #[test]
    fn test_csv_omit_header_uses_fallback_keys_not_empty_rows() {
        // Regression: --omit-header on a CSV used to leave `headers` completely empty
        // (no A1/C01 fallback was ever built), so every row came out as `{}` -- and
        // separately, the `csv` crate's has_headers(true) default silently ate row 0
        // regardless of omit_header, discarding real data.
        let path = write_csv_fixture("csv_omit_header.csv", "SKU001,10\nSKU002,20\n");
        let opts = OptionSet::new(&path).omit_header();
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 2, "both rows present, including the former row 0");
        assert_eq!(rows[0].get("a"), Some(&json!("SKU001")));
        assert_eq!(rows[0].get("b"), Some(&json!(10)));
        assert_eq!(rows[1].get("a"), Some(&json!("SKU002")));
    }

    #[test]
    fn test_csv_header_row_equals_data_row_index_for_predefined_headers() {
        // header_row == data_row_index: a CSV with predefined/external headers (here,
        // via --keys) where no line is actually consumed as a header -- e.g. after
        // skipping 2 notes rows, row 2 is immediately real data, not a header line.
        let path = write_csv_fixture(
            "csv_predefined_headers.csv",
            "Report Title\nGenerated 2026-01-01\nSKU001,10\nSKU002,20\n",
        );
        let mut opts = OptionSet::new(&path).header_row(2).data_row_index(2).omit_header();
        opts.rows.columns = vec![
            Column::new(Some("sku")),
            Column::new(Some("qty")),
        ];
        let rows = process_spreadsheet_direct(&opts).unwrap().to_vec();
        assert_eq!(rows.len(), 2, "both data rows present, notes rows skipped");
        assert_eq!(rows[0].get("sku"), Some(&json!("SKU001")));
        assert_eq!(rows[0].get("qty"), Some(&json!(10)));
        assert_eq!(rows[1].get("sku"), Some(&json!("SKU002")));
    }

    #[test]
    fn test_multisheet_preview_ods() {
        let sample_path = "data/sample-data-2.ods";

        // instantiate the OptionSet with a sample path
        // a maximum row count returned of 10 rows
        // and read mode to *preview* to scan all sheets
        // It should correctly calculate
        let opts = OptionSet::new(sample_path)
            .max_row_count(10)
            .read_mode_preview();

        let result = process_spreadsheet_direct(&opts);

        // The source spreadsheet should have 2 sheets
        let dataset = result.unwrap();
        assert_eq!(dataset.sheets.len(), 2);
        // The source spreadsheet should have 101 + 17 (= 118) populated rows including headers
        assert_eq!(dataset.num_rows, 118);

        // The first sheet's data should only output 10 rows (including the header)
        assert_eq!(dataset.data.first_sheet().len(), 10);
    }

    #[test]
    fn test_column_override_1() {
        let sample_json = json!({
          "sku": "CHAIR16",
          "height": "112cm",
          "width": "69cm",
          "approved": "Y"
        });

        let rows = json_object_to_calamine_data(sample_json);

        let cols = vec![
            Column::new_format(Format::Text, Some(string_value(""))),
            Column::new_format(Format::Float, Some(float_value(95.0))),
            Column::new_format(Format::Float, Some(float_value(65.0))),
            Column::new_format(Format::Truthy, Some(bool_value(false))),
        ];

        // The first sheet's data should only output 10 rows (including the header)
        let opts = &RowOptionSet::simple(&cols);
        let result = workbook_row_to_values(&rows, opts);
        // the second column be cast to 112.0
        assert_eq!(result.get(1).unwrap(), 112.0);
        // the third column be cast to 69.0
        assert_eq!(result.get(2).unwrap(), 69.0);
        // the fourth column be cast to boolean
        assert_eq!(result.get(3).unwrap(), true);
    }

    #[test]
    fn test_column_override_2() {
        let sample_json = json!({
          "name": "Sophia",
          "dob": "2001-9-23",
          "weight": "62kg",
          "result": "GOOD"
        });

        let rows = json_object_to_calamine_data(sample_json);

        let cols = vec![
            Column::new_format(Format::Text, None),
            Column::new_format(Format::Date, None),
            Column::new_format(Format::Float, None),
            // the fourth column be cast to boolean
            Column::new_format(
                Format::truthy_custom("good", "bad"),
                Some(bool_value(false)),
            ),
        ];

        // The first sheet's data should only output 10 rows (including the header)
        let opts = &RowOptionSet::simple(&cols);
        let result = workbook_row_to_values(&rows, opts);
        assert_eq!(result.get(1).unwrap(), "2001-09-23");
        assert_eq!(result.get(2).unwrap(), 62.0);
        assert_eq!(result.get(3).unwrap(), true);
    }

    #[tokio::test]
    async fn test_read_workbook_info() {
        let sample_path = "data/sample-data-1.xlsx";
        let path_data = PathData::new(path::Path::new(sample_path));
        let info = read_workbook_sheet_info(&path_data).await;
        assert!(info.is_ok());
    }

    #[tokio::test]
    async fn test_large_csv_file() {
        let sample_path = "data/large-datasheet.csv";
        let max_rows = 100_000;
        let opts = OptionSet::new(sample_path).max_row_count(max_rows);
        let result = process_spreadsheet_core(&opts, None, None).await;
        if let Ok(data) = result.clone() {
            assert_eq!(data.data.first_sheet().len(), max_rows as usize);
        } else {
            panic!("Failed to process large CSV file");
        }
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_medium_excel_file() {
        let sample_path = "data/medium-spreadsheet-50_000.xlsx";
        let max_rows = 5_000;
        let opts = OptionSet::new(sample_path).max_row_count(max_rows);
        let result = process_spreadsheet_core(&opts, None, None).await;
        if let Ok(data) = result.clone() {
            assert_eq!(data.data.first_sheet().len(), max_rows as usize);
        } else {
            panic!("Failed to process large Excel file");
        }
        assert!(result.is_ok());
    }
}
