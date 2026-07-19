[![mirror](https://img.shields.io/badge/mirror-github-blue)](https://github.com/neilg63/spreadsheet-to-json)
[![crates.io](https://img.shields.io/crates/v/spreadsheet-to-json.svg)](https://crates.io/crates/spreadsheet-to-json)
[![docs.rs](https://docs.rs/spreadsheet-to-json/badge.svg)](https://docs.rs/spreadsheet-to-json)

# spreadsheet-to-json

## Convert Spreadsheets and CSV files to jSON

#### Battle-tested with spreadsheets under 10MB; recent releases handle much larger files. See [version history below](#version-history).

This library crate provides the core functions to convert common spreadsheet and CSV files into JSON or JSONL (JSON Lines) either directly or asynchronously.

It relies on the [Calamine](https://crates.io/crates/calamine) and [CSV](https://crates.io/crates/csv) library crates to process files, the [tokio](https://crates.io/crates/tokio) crate for asynchronous operations and naturally [serde](https://crates.io/crates/serde) and [serde_json](https://crates.io/crates/serde_json) serialization libraries.

It supports the following formats:

- Excel 2007+ Workbook (*.xlsx*)
- Excel 2007+ Macro-Enabled Workbook (*.xlsm*) -- read as plain data; macros are ignored
- Excel 2007+ Binary (*.xlsb*)
- Excel 97-2004 Legacy (*.xls*)
- OpenDocument Spreadsheets (*.ods*) compatible with LibreOffice
- CSV: comma separated values (*.csv*)
- TSV: tab-separated values (*.tsv*)

## Features
- Blazingly fast: It can import 10,000 rows in less than 0.4 seconds.
- Can export to standard JSON or to JSON lines when writing large files
- Formula cells are read as calculated values
- Can identify and convert both Excel's 1900 datetime format and standard ISO format as used in OpenDocument Spreadsheet
- Can identify numeric fields formatted as text and convert them to integers or floats.
- Can identify truthy text or number cells and convert them to booleans
- Can save large files asynchronously

## Core Options

Options can be set by instantiating `OptionSet::new("path/to/spreadsheet.xlsx")` with chained setter methods:

- `.max_row_count(max: u32)`: overrides the default max row count of 10,000. Use this is direct mode or to return only the first *n* rows.
- `.header_row(index: u8)` overrides the default header row index of 0, useful for spreadsheets with a title and notes on top
- `.omit_header()` omit the header altogether and assign default *A1-style* keys or column numbers.
- `.sheet_index(index: u32)` zero-based sheer index. Any value over zero will override the specified sheet name.
- `.sheet_name(name: &str)` case-insensitive sheet name. It will match the first sheet with name after stripping spaces and punctuation.
- `.read_mode_async()` Defer processing of rows with a callback in the second argument in render_spreadsheet_async() 
- `.json_lines()` Output will be rendered one json object per row.
- `field_name_mode(system: &str, override_header: bool)`: use either A1 (`a`, `b`, ... `z`, `aa`, `ab`, ...) or C-prefixed zero-padded numbers (`c01`, `c02`, ...) for the default column key notation where headers are either unavailable or suppressed via the `override_header` flag. The C-style padding width scales with the sheet's total column count, so keys still sort correctly regardless of width: `c01`..`c99` under 100 columns, `c001`..`c999` from 100 up to 1,000, `c0001`..`c9999` from 1,000 up to 10,000 (see `build_padded_col_key` in `headers.rs`).
- `override_headers(keys: &[&str])` Override matched or automatic column keys. More advanced column options will be detailed soon.
- `override_columns(cols: &[Value])` Lets you override column settings, represented here as an array of `serde_json::Value` key/value objects, where :
  - `key`: overrides the header key,
  - `format`: string | integer | float | d1 ... d8 | datetime | date | boolean | truthy | truthy:true_key,false_key 
     - `d1` to `d8` will round floats to the specified max. number of decimal places.
     - `boolean` will cast integer, floats >= 1 as true as well as the strings '1' and 'true', with < 1 and the strings `0` and `false` being false. If unmatched or empty the field value will be null.
     - `truthy` will cast common English-like abbreviations such as Y, Yes as true and N or No false
     - `truthy:true_key,false_key` lets you cast custom strings to true or false. If unmatched the field value will be null.
  - `default`: overrides the default value for empty cells.

The C-style keys are prefixed with `c` rather than left as bare zero-padded numbers (`"001"`, `"002"`, ...) for two reasons: zero-padding alone guarantees the keys sort correctly as plain strings in virtually any programming language, and a bare numeric-looking string is a confusing choice for an object/map key -- it's easy to mistake for an array index, and some languages treat numeric-looking string keys specially. If you actually want positional access instead of keyed access, that's a one-liner in most languages regardless of how the keys are named -- in JavaScript, for example, `Object.values(row)` turns a row object into a plain array of its values in field order.

#### To do
More details of options to come.

Simple example:
```rust 
  let opts = OptionsSet::new("path/to/spreadsheet.ods")
      .sheet_index(3)
      .read_mode_async()
      .override_headers(&["name", "height", "width", "weight", "hue", "price"]);
```

## Core functions

- `process_spreadsheet_direct(opts: &OptionSet)`: May be called in a synchronous context where you need to process results immediately.

- `process_spreadsheet_async(opts: &OptionSet)`: Asynchronously processes files with a callback function to save each row.

## Result set

- `filename`: Matched filename,
- `extension`: Matched extension
- `sheet`: Matched worksheet name and index
- `sheets`: List of available worksheet names
- `keys`: Assigned column keys
- `num_rows`: number of rows in the source file that have been successfully parsed
- `data`: Vector of dynamic objects (IndexMap<String, Value>) that can be easily translated into JSON or other common formats.
- `out_ref`: Optional output reference such as a generated file name, URL or database id.

If the file name and extension cannot be matched, because the file is unavailable or unsupported, the core functions will return a generic error.

### Result Set methods

- `to_json()`: Converts to the result set to `serde_json::Value` that may be printed directly or written to a file.
- `to_output_lines(json_lines: bool)`: Returns a vector of plain-text results with each data row as JSON on a new line
- `rows()`: Returns a vector of rendered JSON strings
- `json_data()`: Returns all data as as `serde_json::Value::Array` ready for conversion or post-processing.

## Examples

The main implementation is my unpublished [Spreadsheet to JSON CLI](https://github.com/neilg63/spreadsheet_to_json_cli) crate,

### Simple immediate jSON conversion

This function processes the spreadsheet file immediately. 

```rust
use spreadsheet_to_json::*;

fn main() -> Result<(), GenericError> {
  let opts = OptionSet::new("path/to/spreadsheet.xlsx").sheet_index(1);
  let result = process_spreadsheet_direct(&opts);
  let json_value = match result {
    Err(msg_code) => json!({ "error": true, "key": msg_code.to_string() }),
    Ok(data_set) => data_set.to_json() // full result set
  };
  println!("{}", json_value);
  Ok(())
}
```

### Preview multiple worksheets

You may preview all worksheets and limit the number of sample rows from each sheet.
```rust
use spreadsheet_to_json::*;

fn main() -> Result<(), GenericError> {
  // set the read mode to PreviewMultiple and limit output to the first 10 data rows
  let opts = OptionSet::new("path/to/spreadsheet-with-2-worksheets.xlsx")
    .read_mode_preview()
    .max_row_count(10)
    .json_lines();

  let result = process_spreadsheet_direct(&opts);
  // output each line
  for line in result.to_output_lines() {
    println!("{}", line);
  }
  Ok(())
}
```

### Asynchronous parsing and saving to a database

This must be called in an async function with a callback to save rows in separate processes.

```rust
use spreadsheet_to_json::*;
use spreadsheet_to_json::tokio;
use indexmap::IndexMap;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), GenericError> {
  let opts = OptionSet::new("path/to/spreadsheet.xlsx").read_mode_async();

  let callback = move |row: IndexMap<String, Value>| -> Result<(), GenericError> {
    save_data_row(row, &connection, dataset_id)
  };

  let result = process_spreadsheet_async(&opts, Box::new(callback), None).await;
  // A successful result set will have an output reference rather than the actual data
  let result_set = match result {
      Err(msg_code) => json!({ "error": true, "key": msg_code.to_string() }),
      Ok(data_set) => data_set.to_json() 
  };
  println!("{}", result_set);
  Ok(())
}

// Save function called in a closure for each row with a database connection and data_id from the outer scope
fn save_data_row(row: IndexMap<String, Value>, connection: &PgConnection, data_id: u32) -> Result<(), GenericError> {
    let mut row_struct = CustomTableStruct {
    id: None,
    data_id: data_id, // or whatever ID setting logic you have
    field1: None,
    field2: None,
    // ... set other fields to None or default values
  };
  
  for (key, value) in row {
    match key.as_str() {
        "field1" => {
            if let Value::String(s) = value {
                row_struct.field1 = Some(s.clone());
            }
        },
        "field2" => {
            if let Value::Number(n) = value {
                if let Some(i) = n.as_i64() {
                    row_struct.field2 = Some(i as i32);
                }
            }
        },
        // Add other field mappings here
        _ => {} // Ignore unknown keys
    }
  }
  diesel::insert_into("data_rows")
  .values(&row_struct)
  .execute(connection)
  .map_err(|_| GenericError("database_error"))?;
  Ok(())
}
```

## Version History <a id="version-history"></a>
- **0.1.2** the core public functions with *Result* return types now use a GenericError error type
- **0.1.3** Refined A1 and C01 column name styles and added result output as vectors of lines for interoperability with CLI utilities and debugging.
- **0.1.4** Added support for the Excel Binary format (.xlsb)
- **0.1.5** Added two new core functions `process_spreadsheet_direct()` for direct row processing in a synchronous context and `process_spreadsheet_async()`  in an asynchronous context with a callback. If you need to process a spreadsheet directly in an async function
- **0.1.6** Deprecated public function beginning with render (render_spreadsheet_direct() has become. You should use `process_spreadsheet_immediate()` for immediate processing of spreadsheets in an async context). Ensured the header row does not appear as the first data row in spreadsheets.
- **0.1.7** Added support for multiple worksheets in preview mode and refined output options
- **0.1.10** Reviewed date-time parsing options for Excel and OpenDocument spreadsheets. Reorganised cell post-processors by detected data-type.
- **0.2.0** Bumped `calamine` to 0.36.0. Switched `is-truthy`, `alphanumeric` and `simple-string-patterns` from local path dependencies to their published crates.io versions — `is-truthy` gained a `TruthyRuleSet` builder (`options()`/`true_options()`/`false_options()`, replacing the old `TruthyOption` vector plumbing). Fixed the CSV reader silently ignoring its own default row cap (`DEFAULT_MAX_ROWS`) when no explicit `.max_row_count()` was set — large uploads could load unbounded into memory. Fixed a panic in `Column::from_json` on a non-string `format` field. Assorted performance fixes: redundant `Column`/`Format` clones per cell, a fresh `Arc` allocated per CSV row instead of once per file, a double clone in `ResultSet::rows()`. Fixed a panic in `read_workbook_sheet_info` on an unreadable individual sheet.
- **0.2.1** Bumped `is-truthy` to 0.1.2, fixing CSV cells like `"SKU001"` or a date starting `"01/..."` being silently coerced to the boolean `true`/`false` just because they contained an embedded `0` or `1`. Added `Column::source_key` (and `Column::from_source_key_with_format()`): a column override can now be matched by its natural, auto-detected key wherever that column actually is, instead of strictly by position — see `resolve_columns()`/`natural_column_keys()` in `headers.rs`. Overrides with no `source_key` still apply positionally, unchanged. Fixed `Format::Integer` on a CSV cell with a decimal value (e.g. `"58.2"`) silently producing `0` instead of the truncated integer.
- **0.2.2** Fixed a per-column `Format::Date`/`Format::DateTime` override having no effect at all on real (non-string) datetime cells in xlsx/ods — only the row-wide `--date-only`-style default was ever consulted for `Data::DateTime`/`Data::DateTimeIso` cells, so casting a single datetime column to date-only silently did nothing; see `resolve_date_only()` in `reader.rs`. Fixed `is_not_header_row` misclassifying the header row as real data (leaking it into the output as a bogus extra row) whenever any column had a non-`Auto` `Format` — it used to compare the row's already-formatted values against the header text, and formatting the header row's own text (e.g. through a date or decimal parse) commonly turns it into `null` or something else that no longer matches; it now compares raw, un-coerced cell text instead. Both bugs affected the xlsx/ods path only, not CSV/TSV.
- **0.2.3** Added `.xlsm` (macro-enabled Excel workbook) support. `.xlsm` uses the exact same OOXML container as `.xlsx` and `calamine` has always read both through its Xlsx reader, but this crate's own `Extension` enum only recognised the `.xlsx` extension before.
- **0.3.0** Breaking: `Column.date_only: bool` is now `Column.datetime_mode: DateTimeMode`, and `RowOptionSet`'s separate `date_only`/`time_only` booleans are now a single `datetime_mode: DateTimeMode` field -- `RowOptionSet::new()` and `Column::from_source_key_with_format()`/`from_key_ref_with_format()` take `DateTimeMode` instead of the old `bool` in their signatures. Added a `DateTimeMode` enum (`Full`, `Simple`, `DateOnly`, `TimeOnly`, `HmOnly`) as the single, unified representation for datetime rendering, replacing `RowOptionSet`'s previous separate `date_only`/`time_only` booleans and the previously dead `Column.date_only: bool` field — that field was fully wired up for JSON/display output but never actually consulted by the rendering logic, so a per-column date-only override on a genuine (non-string) datetime cell silently did nothing; it's now `Column.datetime_mode`, correctly scoped to `Format::Auto` columns whose cells are already genuine datetimes (`Data::DateTime`/`Data::DateTimeIso`) — it never touches strings or numbers in the same column, unlike an explicit `Format` override, which forces date/time interpretation onto every cell type. Added `Format::Time` (`ti`/`time`), `Format::Hm` (`hm`), and `Format::DateTimeSimple` (`ds`/`simple`) alongside the existing `Format::Date`/`Format::DateTime`, giving explicit per-column control over precision: full ISO-8601 with milliseconds and a trailing `Z` (the default, for JS/JSON interop), the same without the milliseconds/`Z`, date-only, time-only with seconds, or hours:minutes only. Precedence is a column's own `Format` override, then its own `datetime_mode` (Auto columns only), then the row-wide `RowOptionSet.datetime_mode` default. Fixed genuinely time-only Excel cells — e.g. a cell formatted as plain `hh:mm`, which Excel actually stores as a full datetime serial with zero elapsed days, since it has no true time-only type — rendering with the meaningless placeholder date `1899-12-31` (the 1900 date system's epoch); a cell with no real date component is now auto-detected and rendered as a bare time whenever a full datetime wasn't explicitly requested. Fixed `csv_cell_to_json_value` having no `Format::Date`/`Format::DateTime` handling at all — a date-like CSV string starting with a number (e.g. `"2023-06-15"`) could be misread by the numeric-extraction path before ever reaching `fuzzy-datetime`; all five date/time formats now apply uniformly across native Excel/ODS datetime cells, ISO datetime strings, and CSV/TSV text cells. Bumped `fuzzy-datetime` to 0.1.3, adding sliding-pivot two-digit-year expansion (e.g. `21-06-23` resolves relative to the current date rather than a fixed 50/50 century split) for ambiguous CSV/plain-text date cells, and switching its license to MIT.
- **0.3.1** `ResultSet::new()` takes two new required parameters, `header_row_index: Option<usize>` and `body_start_index: usize`, and `ResultSet` gains matching public fields. Fixed a gap where there was no way to learn which row indices a read actually used -- `OptionSet.header_row`/`.data_row_index` only ever reflect an *explicit* override and stay `None` whenever auto-detection resolved them instead, so a caller relying on those fields (e.g. `spread-cli`'s `--json` metadata) saw `null` even when a header and data start were successfully detected and used. `ResultSet.header_row_index`/`.body_start_index` now carry the actually-resolved 0-based indices regardless of whether they came from an explicit override or `detect_header_and_data_rows`. Scoped to the single-sheet read paths (`read_single_worksheet`/`read_csv_core`); `ResultSet::from_multiple()` (the `--preview`/multi-sheet path) still reports `None`/`0` for now, since a single pair of indices can't represent multiple sheets that may each resolve differently.
- **0.3.2** Fixed `Format::Decimal(places)` (the `d1`-`d8` `--keys` format codes) having no effect at all on genuine numeric cells -- `process_float_value` (native xlsx/ods float cells) and `csv_cell_to_json_value`'s numeric branch (CSV/TSV) both matched `Format::Integer`/`Format::Boolean` explicitly but fell through to a plain, unrounded `Value::Number` for everything else, silently ignoring the requested precision. `Format::from_str` already parsed `"d1"`.."d8"` into `Format::Decimal(n)` correctly, and `process_string_value` already rounded correctly for string-typed cells -- the two missing spots now use the same `round_decimal`/`float_value` helpers to match. Fixed `Format::Time`/`Format::Hm` returning `null` for a string cell that's already just a bare time with no date component at all (e.g. `"11:39"`, common for a column pre-formatted as text) -- `iso_fuzzy_to_datetime_string` requires a date to parse anything, so it always failed on a bare time and the whole value fell through to the column's default. A new `parse_bare_time_string` helper is tried as a fallback only once that full-datetime parse has already failed, so a genuine full datetime string is completely unaffected (it always succeeds via the original path first). Deliberately a loose first-pass gate, not a validating time parser -- starts with a digit, ends with a digit, contains a `:`, passed through unchanged -- using `simple-string-patterns`' existing `CharType`/`starts_with_type`/`ends_with_type` rather than pulling in regex for one narrow case; stricter hh:mm(:ss) shape/range validation or reformatting is left to a post-processor downstream (`jq`/`yq`, or a regex-capable tool). Fixed `Format::Date`/`Format::DateTime`/`Format::DateTimeSimple`/`Format::Time`/`Format::Hm` on text cells and CSV values silently rejecting any `/`-separated date (by far the most common date separator worldwide), regardless of order -- even the unambiguous `"2026/07/19"` (right order, "wrong" separator) returned `null`. Root cause: `process_string_value`/`csv_cell_to_json_value` called `iso_fuzzy_to_date_string`/`iso_fuzzy_to_datetime_string`, which force `DateOrder::YMD` and `'-'` as the *only* accepted separator (`DateOptions::default()`) and, because an explicit `DateOptions` is passed, skip fuzzy-datetime's own order/separator guessing entirely -- appropriate for `process_iso_datetime_value`'s already-unambiguous calamine-provided ISO datetime strings (left unchanged), wrong for arbitrary user-typed text. New `guess_date_string`/`guess_datetime_string` wrappers call fuzzy-datetime's guessing entry points (`fuzzy_to_date_string`/`fuzzy_to_datetime_string_opts` with `date_opts: None`) instead -- output is always the same canonical `YYYY-MM-DD`/`YYYY-MM-DDTHH:MM:SS.mmmZ` shape regardless of the input's order or separator; only what's *accepted* as input is more flexible. The working `'-'`-separated case is unaffected (verified byte-for-byte identical). Bumped `fuzzy-datetime` to 0.1.4, which the fix above depends on for two of its own bugs: dot-separated dates (e.g. `"19.07.2026"`) weren't recognised even under full order-guessing, because `segment_is_subseconds` misread a bare 4-digit year as milliseconds-plus-timezone-suffix (it only checked the trailing character was *alphanumeric*, true for a digit too, rather than genuinely non-numeric like the `Z` in `"678Z"`) and silently chopped the year off before order-guessing ever saw it; and a date+time string where the date parsed but the time chunk didn't (e.g. `"2026-07-19 11.39"`, dot instead of colon) produced a malformed, dangling `"2026-07-19T"` instead of correctly failing the whole parse, because a time-parse failure was silently discarded via `.unwrap_or_default()` instead of propagated. Added: `Format::Time`/`Format::Hm` now reconstruct `HH:MM` from a plain decimal number too -- the common real-world case where a user typed a dot-separated time like `"12.30"` (meaning 12:30) and the spreadsheet app, having no dedicated time type for a dot-typed value, silently read it as the decimal `12.3` (trailing zero dropped). Two different mechanisms, split by whether the trailing-zero information actually survives: a genuine native xlsx/ods float cell (`process_float_value`) has already lost it by the time this runs (`12.30` and `12.3` are the same `f64`), so a new `decimal_to_hm_string` reads the digits after the decimal point *literally* as MM (`0.3` -> `"30"`, restoring the dropped trailing zero) rather than scaling them by 60 (`0.3` of an hour is 18 minutes, not 30), and validates the result is a plausible time (hours `< 24`, minutes `< 60`) before accepting it -- a genuine decimal quantity like a price (`12.75`) correctly comes back `null`. A CSV/text-cell value hasn't lost anything (a string preserves exactly what was typed), so it needs neither reconstruction nor validation -- just `.replace('.', ":")` and a second pass through the already-loose `parse_bare_time_string` gate from above, same "no range checking, an explicit override means you know what you're asserting" philosophy (`"12.75"` as literal text under `|time` comes back `"12:75"`, not `null` -- unlike the float case). `HH:MM` only either way, deliberately -- there's no way to recover seconds from a single decimal fraction. Opt-in only, via an explicit `--keys "col|time"`/`"col|hm"` override or `Column::datetime_mode` -- never applied automatically to an arbitrary numeric column. `parse_bare_time_string` was then reworked to extract its numeric components directly via `alphanumeric`'s `to_numbers::<u8>()` (already a transitive dependency, no regex) instead of a loose "starts/ends with a digit" character check -- getting real hh:mm(:ss) range validation essentially for free (previously a clearly-implausible `"24:00"` or `"11:60"` passed straight through as a bogus "valid" time; now correctly rejected) and consistent zero-padded output (`"9:5:3"` -> `"09:05:03"`, not passed through as typed). The dot-typed-time case (`"12.30"`) is now handled by the same function directly (`.` swapped for `:` before extraction) rather than a second fallback call. Both `parse_bare_time_string` and `decimal_to_hm_string` no longer cap hours at `< 24` -- a "time" column is just as often a duration (elapsed hours: a race, a video, a timesheet total) as a time-of-day, and durations routinely exceed 23 hours (`"27:45:00"`, a decimal `100.3` meaning `100:30`); there's no way to tell which a given column means from the value alone, so this no longer guesses and rejects perfectly valid durations. Minutes/seconds stay bounded to `< 60` either way, and `decimal_to_hm_string` still rejects negative values. `parse_bare_time_string` now also rejects any string containing a letter before ever calling `to_numbers::<u8>()` -- that function silently discards non-digit characters, so without this guard an AM/PM-suffixed time (`"2:30 PM"`) would extract as `[2, 30]` and format as `"02:30"`, which isn't a rejection, it's a *wrong* answer (should be 14:30) indistinguishable from a correct one -- it happens to coincidentally match the 24-hour value for `"11:39 AM"`/`"12:00 PM"` and silently doesn't for everything else. Real 12-hour/AM-PM conversion (and duration-unit notation like `"2h45m"`) stay out of scope; sanitizing input from a mistaken data type means rejecting it here, not silently mangling it into something plausible-looking. Added: `parse_bare_time_string` now actually converts a trailing AM/PM marker instead of rejecting it -- detected up front via `ends_with_ci("am")`/`ends_with_ci("pm")` (so the letters-reject guard above no longer fires on it), then the extracted hour is reinterpreted as 12-hour-clock: `"2:30 PM"` -> `"14:30"`, `"12:45am"` -> `"00:45"` (the 12-hour-clock oddity where 12am is midnight), `"12:00 PM"` (noon) stays `"12:00"`. An hour outside 1-12 alongside an am/pm marker (`"14:30pm"`) is contradictory 12-hour notation, not real-world input, so it's rejected rather than guessed at -- same sanitize-don't-mangle philosophy as before, just now able to actually resolve the one ambiguous case (AM/PM) instead of only ever refusing it. Duration-unit notation (`"2h45m"`) and any other lettered input remain rejected exactly as before.
