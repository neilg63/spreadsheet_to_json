use heck::ToSnakeCase;
use indexmap::IndexMap;
use serde_json::{json, Error, Value};
use simple_string_patterns::{SimpleMatch, StripCharacters};
use to_segments::ToSegments;
use std::{path::Path, str::FromStr, sync::Arc};

use is_truthy::TruthyRuleSet;
/// default max number of rows in direct single sheet mode without an override via ->max_row_count(max_row_count)
pub const DEFAULT_MAX_ROWS: usize = 10_000;
/// default max number of rows multiple sheet preview mode without an override via ->max_row_count(max_row_count)
pub const DEFAULT_MAX_ROWS_PREVIEW: usize = 1000;

/// How a datetime-bearing cell is rendered. `Full` is the ordinary complete ISO datetime;
/// the other three each discard progressively more of it. Used both as `RowOptionSet`'s
/// row-wide default and as `Column`'s per-column override for genuine datetime cells --
/// see the doc comments on each for how the two combine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DateTimeMode {
  #[default]
  Full, // complete date and time with milliseconds and a trailing Z, e.g.
        // "2023-06-15T10:17:00.000Z" -- the default, JS-interop-friendly form
  Simple, // complete date and time, but without milliseconds or a trailing Z, e.g.
          // "2023-06-15T10:17:00"
  DateOnly, // date component only, e.g. "2023-06-15"
  TimeOnly, // time-of-day only, with seconds, e.g. "10:17:00"
  HmOnly, // time-of-day only, hours and minutes, e.g. "10:17" -- for values better read as
          // a plain clock time (a start/end time, a recurring daily slot) than a precise
          // duration down to the second
}

impl std::fmt::Display for DateTimeMode {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let result = match self {
      Self::Full => "date/time",
      Self::Simple => "simple date/time",
      Self::DateOnly => "date only",
      Self::TimeOnly => "time only",
      Self::HmOnly => "hours:minutes only",
    };
    write!(f, "{}", result)
  }
}

/// Row parsing options with nested column options
#[derive(Debug, Clone, Default)]
pub struct RowOptionSet {
  pub columns: Vec<Column>,
  pub decimal_comma: bool, // always parse as euro number format
  /// Row-wide default rendering mode for datetime cells (genuine `Data::DateTime`/
  /// `Data::DateTimeIso` cells, and any string/CSV cell under an explicit
  /// `Format::Date`/`Format::Time`/`Format::Hm`/`Format::DateTime` override). A column's
  /// own `Format` override, or its own `datetime_mode` on a `Format::Auto` column, takes
  /// precedence over this row-wide default; see `Column::datetime_mode`.
  pub datetime_mode: DateTimeMode,
}

impl RowOptionSet {

  // simple constructor with column keys only
  pub fn simple(cols: &[Column]) -> Self {
    RowOptionSet {
      decimal_comma: false,
      datetime_mode: DateTimeMode::Full,
      columns: cols.to_vec()
    }
  }

  // lets you set all options
  pub fn new(cols: &[Column], decimal_comma: bool, datetime_mode: DateTimeMode) -> Self {
    RowOptionSet {
      decimal_comma,
      datetime_mode,
      columns: cols.to_vec()
    }
  }

  pub fn column(&self, index: usize) -> Option<&Column> {
    self.columns.get(index)
  }

  pub fn date_mode(&self) -> String {
    self.datetime_mode.to_string()
  }

  pub fn decimal_separator(&self) -> String {
    if self.decimal_comma {
      ","
    } else {
      "."
    }.to_string()
  }
}

/// Core options with nested row options
#[derive(Debug, Clone, Default)]
pub struct OptionSet {
  pub selected: Option<Vec<String>>, // Optional sheet name reference. Will default to index value if not matched
  pub indices: Vec<u32>, // worksheet index
  pub path: Option<String>, // path argument. If None, do not attempt to parse
  pub rows: RowOptionSet,
  pub jsonl: bool,
  pub max: Option<u32>,
  pub omit_header: bool,
  /// 0-based row index of the header row. `None` means unset -- when `data_row_index` is
  /// also unset (and headers aren't omitted), the reader runs a best-guess detection
  /// pass instead of blindly assuming row 0; see `detect::detect_header_and_data_rows`.
  pub header_row: Option<usize>,
  /// 0-based row index where actual data begins. `None` (the default) means immediately
  /// after the header row (or triggers detection, per `header_row`'s doc above). Rows
  /// strictly between the header row and this one are skipped entirely -- neither
  /// captured as headers nor as data -- for spreadsheets that leave a note, blank, or
  /// subtitle row between the header and the first real data row.
  pub data_row_index: Option<usize>,
  /// Whether to run best-guess header/data-row detection (see
  /// `detect::detect_header_and_data_rows`) when both `header_row` and `data_row_index`
  /// are unset, instead of assuming row 0 is the header. Off (`false`) by default for
  /// direct library use, so `OptionSet::new(path)` alone always behaves the same simple,
  /// predictable way it always has -- callers that want detection opt in explicitly via
  /// `.detect_header()`. Consumers like `spread-cli` that want it as *their own* default
  /// user experience turn it on unconditionally when building their `OptionSet`.
  pub detect_header: bool,
  pub read_mode: ReadMode,
  pub field_mode: FieldNameMode
}

impl OptionSet {
  /// Instantiates a new option set with a path string for file operations.
  pub fn new(path_str: &str) -> Self {
    OptionSet {
        selected: None,
        indices: vec![0],
        path: Some(path_str.to_string()),
        rows: RowOptionSet::default(),
        jsonl: false,
        max: None,
        omit_header: false,
        header_row: None,
        data_row_index: None,
        detect_header: false,
        read_mode: ReadMode::Sync,
        field_mode: FieldNameMode::AutoA1,
    }
  }

  /// Sets the sheet name for the operation.
  pub fn sheet_name(mut self, name: &str) -> Self {
    self.selected = Some(vec![name.to_string()]);
    self
  }

  /// Sets the sheet name for the operation.
  pub fn sheet_names(mut self, names: &[String]) -> Self {
    self.selected = Some(names.to_vec());
    self
  }

  /// Sets the sheet index.
  pub fn sheet_index(mut self, index: u32) -> Self {
      self.indices = vec![index];
      self
  }

  /// Sets the sheet index.
  pub fn sheet_indices(mut self, indices: &[u32]) -> Self {
    self.indices = indices.to_vec();
    self
}

  /// Sets JSON Lines mode to true.
  pub fn json_lines(mut self) -> Self {
      self.jsonl = true;
      self
  }

  /// Sets JSON Lines mode
  pub fn set_json_lines(mut self, mode: bool) -> Self {
    self.jsonl = mode;
    self
  }

  /// Omits the header when reading.
  pub fn omit_header(mut self) -> Self {
      self.omit_header = true;
      self
  }

  /// Sets the header row's 0-based row index.
  pub fn header_row(mut self, row: usize) -> Self {
      self.header_row = Some(row);
      self
  }

  /// Sets the 0-based row index where actual data begins. Rows between the header row
  /// and this one are skipped entirely, for spreadsheets that leave a note or blank row
  /// between the header and the first real data row. If set at or before the header row,
  /// this is ignored and data capture falls back to starting immediately after the
  /// header row instead.
  pub fn data_row_index(mut self, row: usize) -> Self {
      self.data_row_index = Some(row);
      self
  }

  /// Opts into best-guess header/data-row detection when both `header_row` and
  /// `data_row_index` are left unset, instead of the library's normal default of
  /// assuming row 0 is the header. Off by default -- see the `detect_header` field doc.
  pub fn detect_header(mut self) -> Self {
      self.detect_header = true;
      self
  }

  /// Sets the maximum number of rows to read.
  pub fn max_row_count(mut self, max: u32) -> Self {
      self.max = Some(max);
      self
  }

  /// Sets the read mode to asynchronous, single sheet mode
  /// This is for reading long files with 10K+ rows in the target sheet
  pub fn read_mode_async(mut self) -> Self {
      self.read_mode = ReadMode::Async;
      self
  }

   /// Sets the read mode to direct with multiple sheet output
   /// This serves to fetch quick a overview of a spreadsheet
   pub fn read_mode_preview(mut self) -> Self {
    self.read_mode = ReadMode::PreviewMultiple;
    self
}

  /// Sets read mode from a range of common key names
  /// async, preview or sync (default) with synonyms such as `a`, `p` and `s`
  /// If the key is unmatched, it will always default to Sync
  pub fn set_read_mode(mut self, key: &str) -> Self {
    self.read_mode = ReadMode::from_key(key);
    self
  }

  pub fn multimode(&self) -> bool {
    self.read_mode.is_multimode()
  }

  pub fn file_name(&self) -> Option<String> {
    if let Some(path_str) = self.path.clone() {
      Path::new(&path_str).file_name().map(|f| f.to_string_lossy().to_string())
    } else {
      None
    }
  }

  /// Override matched and unmatched headers with custom headers.
  pub fn override_headers(mut self, keys: &[&str]) -> Self {
    let mut columns: Vec<Column> = Vec::with_capacity(keys.len());
    for ck in keys {
        columns.push(Column::new(Some(&ck.to_snake_case())));
    }
    self.rows = RowOptionSet::simple(&columns);
    self
  }

  /// Override matched and unmatched columns with custom keys and/or formatting options
  pub fn override_columns(mut self, cols: &[Value]) -> Self {
    let mut columns: Vec<Column> = Vec::with_capacity(cols.len());
    for json_value in cols {
        columns.push(Column::from_json(json_value));
    }
    self.rows = RowOptionSet::simple(&columns);
    self
  }

  /// Sets the column key naming convention.
  pub fn field_name_mode(mut self, system: &str, override_header: bool) -> Self {
      self.field_mode = FieldNameMode::from_key(system, override_header);
      self
  }

  pub fn row_mode(&self) -> String {
    if self.jsonl {
      "JSON lines"
    } else {
      "JSON"
    }.to_string()
  }

  pub fn header_mode(&self) -> String {
    if self.omit_header {
      "ignore"
    } else {
      "capture"
    }.to_string()
  }

  /// render option output contextually as JSON
  pub fn to_json(&self) -> Value {
    
    let mut output: IndexMap<String, Value> = IndexMap::new();
    if let Some(selected) =  self.selected.clone() {
      let selected = if self.multimode() {
        json!({
          "sheets": selected,
          "indices": self.indices.clone()
        })
      } else {
        json!({
          "sheet": selected.first().unwrap_or(&"".to_string()),
          "index": self.indices.first().unwrap_or(&0)
        })
      };
      output.insert("selected".to_string(), selected);
    }
    if let Some(fname) = self.file_name() {
      output.insert("file name".to_string(), fname.into());
    }
    if let Some(max_val) = self.max {
      output.insert("max".to_string(), max_val.into());
    }
    output.insert("omit_header".to_string(), self.omit_header.into());
    output.insert("header_row".to_string(), self.header_row.into());
    output.insert("data_row_index".to_string(), self.data_row_index.into());
    output.insert("detect_header".to_string(), self.detect_header.into());
    output.insert("read_mode".to_string(), self.read_mode.to_string().into());
    output.insert("jsonl".to_string(), self.jsonl.into());
    output.insert("decimal_separator".to_string(), self.rows.decimal_separator().into());
    output.insert("date_mode".to_string(), self.rows.date_mode().into());
    if !self.columns().is_empty() {
      let columns: Vec<Value> = self.rows.columns.clone().into_iter().map(|c| c.to_json()).collect();
      output.insert("columns".to_string(), columns.into());
    }
    json!(output)
  }

  pub fn index_list(&self) -> String {
    self.indices.clone().into_iter().map(|s| s.to_string()).collect::<Vec<String>>().join(", ")
  }

  /// render option output contextually as a list of strings
  /// for use in a terminal or text output
  pub fn to_lines(&self) -> Vec<String> {
    let mut lines = vec![];
    if let Some(s_names) = self.selected.clone() {
      let plural = if s_names.len() > 1 {
        "s"
      } else {
        ""
      };
      lines.push(format!("sheet name{}: {}", plural, s_names.join(",")));
    } else if !self.indices.is_empty() {
      lines.push(format!("sheet indices: {}", self.index_list()));
    }
    if let Some(fname) = self.file_name() {
      lines.push(format!("file name: {}", fname));
    }
    if self.max.is_some() {
      let max_val = self.max.unwrap_or(0);
      if max_val > 0 {
        lines.push(format!("max rows: {}", max_val));
      }
    }
    lines.extend(vec![
      format!("mode: {}", self.row_mode()),
      format!("headers: {}", self.header_mode()),
      format!("header row: {}", self.header_row.map(|v| v.to_string()).unwrap_or_else(|| if self.detect_header { "auto-detect".to_string() } else { "0 (default)".to_string() })),
      format!("data row index: {}", self.data_row_index.map(|v| v.to_string()).unwrap_or_else(|| "default (immediately after header)".to_string())),
      format!("decimal separator: {}", self.rows.decimal_separator()),
      format!("date mode: {}", self.rows.date_mode()),
      format!("column style: {}", self.field_mode.to_string())
    ]);

    if !self.columns().is_empty() {
      lines.push("columns:".to_string());
      for col in self.rows.columns.clone() {
        lines.push(col.to_line());
      }
    }
    lines
  }

  /// 0-based header row index, resolved *without* auto-detection -- `header_row` if set,
  /// row 0 otherwise. Detection (see `detect::detect_header_and_data_rows`) only runs
  /// when both `header_row` and `data_row_index` are unset, and requires sample row data
  /// this method doesn't have access to; readers check for that case themselves before
  /// falling back to this method.
  pub fn header_row_index(&self) -> usize {
    self.header_row.unwrap_or(0)
  }

  /// 0-based absolute row index at which data capture may begin, combining `header_row`,
  /// `data_row_index`, and `omit_header`. `None` when nothing is customized (header row
  /// 0, no explicit data row) -- meaning no additional gating beyond the ordinary "first
  /// row after the header" behavior applies, so callers can skip this check entirely
  /// rather than compute a value that changes nothing.
  ///
  /// `data_row_index` is honored literally whenever it's at or after the header row --
  /// including *equal to* the header row, which is a legitimate (if rare) configuration:
  /// a CSV with predefined/external headers where no line is actually consumed as a
  /// header, or a sheet that inherits its column names from elsewhere and has no header
  /// line of its own. It's only treated as unset (falling back to the default below) when
  /// it's strictly *before* the header row, which is never meaningful.
  ///
  /// The default when `data_row_index` is unset depends on whether a header row is
  /// actually being consumed: immediately after the header row when one is (the common
  /// case), or right at the header row itself when `omit_header` is set -- since then no
  /// row is being consumed for headers in the first place, so there's nothing to skip
  /// past.
  pub fn first_data_row_index(&self) -> Option<usize> {
    if self.header_row.is_none() && self.data_row_index.is_none() {
      return None;
    }
    let header_row_index = self.header_row_index();
    let default_start = if self.omit_header { header_row_index } else { header_row_index + 1 };
    match self.data_row_index {
      Some(requested) if requested >= header_row_index => Some(requested),
      _ => Some(default_start),
    }
  }

  /// get the maximum of rows to be output synchronously
  pub fn max_rows(&self) -> usize {
    if let Some(mr) = self.max {
      mr as usize
    } else {
      match self.read_mode {
        ReadMode::PreviewMultiple => DEFAULT_MAX_ROWS_PREVIEW,
        _ => DEFAULT_MAX_ROWS
      }
    }
  }

  /// future development with advanced column options
  #[allow(dead_code)]
  pub fn columns(&self) -> Vec<Column> {
    self.rows.columns.clone()
  }

  /// cloned read mode
  pub fn read_mode(&self) -> ReadMode {
    self.read_mode
  }

  /// Needs full data set to processed later
  pub fn is_async(&self) -> bool {
    self.read_mode.is_async()
  }

  // Should rows be captured synchronously
  pub fn capture_rows(&self) -> bool {
    !matches!(self.read_mode, ReadMode::Async)
  }

}


/// Cell format overrides
#[derive(Debug, Clone)]
pub enum Format {
  Auto, // automatic interpretation
  Text, // text
  Integer, // integer only
  Decimal(u8), // decimal to stated precision
  Float, // f64 
  Boolean, // Boolean or  cast to boolean from integers
  Date, // Interpret as date only
  DateTime, // Interpret as full datetime
  DateTimeSimple, // Interpret as full datetime, without milliseconds or a trailing Z
  Time, // Interpret as time-of-day only, discarding any date component
  Hm, // Interpret as hours:minutes only, discarding seconds and any date component
  DateTimeCustom(Arc<str>),
  Truthy, // interpret common yes/no, y/n, true/false text strings as true/false
  #[allow(dead_code)]
  TruthyCustom(TruthyRuleSet) // define custom yes/no values
}

impl std::fmt::Display for Format {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let result = match self {
      Self::Auto => "auto".to_string(),
      Self::Text => "text".to_string(),
      Self::Integer => "integer".to_string(),
      Self::Decimal(n) => format!("decimal({})", n),
      Self::Float => "float".to_string(),
      Self::Boolean => "boolean".to_string(),
      Self::Date => "date".to_string(),
      Self::DateTime => "datetime".to_string(),
      Self::DateTimeSimple => "simple".to_string(),
      Self::Time => "time".to_string(),
      Self::Hm => "hm".to_string(),
      Self::DateTimeCustom(fmt) => format!("datetime({})", fmt),
      Self::Truthy => "truthy".to_string(),
      Self::TruthyCustom(rules) => {
        let true_str: Vec<String> = rules.true_options().iter().map(|o| o.pattern().to_string()).collect();
        let false_str: Vec<String> = rules.false_options().iter().map(|o| o.pattern().to_string()).collect();
        format!("truthy({},{})", true_str.join("|"), false_str.join("|"))
      },
    };
    write!(f, "{}", result)
  }
}

impl FromStr for Format {
  type Err = Error;
  fn from_str(key: &str) -> Result<Self, Self::Err> {
      let fmt = match key {
        "s" | "str" | "string" | "t" | "txt" | "text" => Self::Text,
        "i" | "int" | "integer" => Self::Integer,
        "d1" | "decimal_1" => Self::Decimal(1),
        "d2" | "decimal_2" => Self::Decimal(2),
        "d3" | "decimal_3" => Self::Decimal(3),
        "d4" | "decimal_4" => Self::Decimal(4),
        "d5" | "decimal_5" => Self::Decimal(5),
        "d6" | "decimal_6" => Self::Decimal(6),
        "d7" | "decimal_7" => Self::Decimal(7),
        "d8" | "decimal_8" => Self::Decimal(8),
        "fl" | "f" | "float" => Self::Float,
        "b" | "bool" | "boolean" => Self::Boolean,
        "da" | "date" => Self::Date,
        "dt" | "datetime" => Self::DateTime,
        "ds" | "simple" | "datetime_simple" => Self::DateTimeSimple,
        "ti" | "time" => Self::Time,
        "hm" | "hoursminutes" => Self::Hm,
        "tr" | "truthy" => Self::Truthy,
        _ => {
          if let Some(str) = match_custom_dt(key) {
            Self::DateTimeCustom(Arc::from(str))
          } else if let Some((yes, no)) = match_custom_truthy(key) {
            Self::TruthyCustom(TruthyRuleSet::new().add_true(&yes).add_false(&no))
          } else {
            Self::Auto
          }
        },
      };
      Ok(fmt)
  }
}

fn match_custom_dt(key: &str) -> Option<String> {
  let test_str = key.trim();
  if test_str.starts_with_ci("dt:") {
    Some(test_str[3..].to_string())
  } else {
    None
  }
}

fn match_custom_truthy(key: &str) -> Option<(String,String)> {
  let test_str = key.trim();
  if let (Some(head), Some(tail)) = test_str.to_head_tail(":") {
    if tail.len() > 1 && head.len() > 1 && head.starts_with_ci("tr") {
      if let (Some(yes), Some(no)) = tail.to_head_tail(",") {
        if !yes.is_empty() && !no.is_empty() {
          return Some((yes.to_string(), no.to_string()));
        }
      }
    }
  }
  None
}

impl Format {
  #[allow(dead_code)]
  pub fn truthy_custom(yes: &str, no: &str) -> Self {
    Format::TruthyCustom(TruthyRuleSet::new().add_true(yes).add_false(no))
  }
}

/// Reads a column's per-column `DateTimeMode` from JSON, either via an explicit
/// `"datetime_mode": "date"/"time"/"hm"/"full"` string, or (for backwards compatibility
/// with configs predating `DateTimeMode`) the boolean keys `"date_only"`/`"time_only"`/
/// `"hm_only"`, checked in that order of precedence.
fn datetime_mode_from_json(json: &Value) -> DateTimeMode {
  if let Some(mode_str) = json.get("datetime_mode").and_then(|v| v.as_str()) {
    return match mode_str {
      "date" | "date_only" => DateTimeMode::DateOnly,
      "time" | "time_only" => DateTimeMode::TimeOnly,
      "hm" | "hm_only" => DateTimeMode::HmOnly,
      _ => DateTimeMode::Full,
    };
  }
  if json.get("date_only").and_then(|v| v.as_bool()).unwrap_or(false) {
    DateTimeMode::DateOnly
  } else if json.get("time_only").and_then(|v| v.as_bool()).unwrap_or(false) {
    DateTimeMode::TimeOnly
  } else if json.get("hm_only").and_then(|v| v.as_bool()).unwrap_or(false) {
    DateTimeMode::HmOnly
  } else {
    DateTimeMode::Full
  }
}

#[derive(Debug, Clone)]
pub struct Column {
  pub key:  Option<Arc<str>>,
  /// Natural (auto-detected, snake_cased) key to match this override against, regardless
  /// of the column's actual position. When None, the column applies positionally instead
  /// (matched by its index within the configured column list), as before.
  pub source_key: Option<Arc<str>>,
  pub format: Format,
  pub default: Option<Value>,
  /// Rendering mode applied *only* when this column's own `format` is `Format::Auto` and
  /// the source cell is already a genuine datetime (`Data::DateTime`/`Data::DateTimeIso`)
  /// -- it has no effect on strings or numbers in that column, unlike
  /// `Format::Date`/`Format::Time`/`Format::Hm`/`Format::DateTime`, which force *any*
  /// cell type through date/time interpretation. Overrides the row-wide
  /// `RowOptionSet::datetime_mode` default when set to anything other than `Full`.
  pub datetime_mode: DateTimeMode,
  pub decimal_comma: bool, // parse as euro number format
}

impl Column {

  /// build new column with an optional key name only
  pub fn new(key_opt: Option<&str>) -> Self {
    Self::from_key_ref_with_format(key_opt, Format::Auto, None, DateTimeMode::Full, false)
  }

  /// build new column data type override and optional default
  pub fn new_format(fmt: Format, default: Option<Value>) -> Self {
    Self::from_key_ref_with_format(None, fmt, default, DateTimeMode::Full, false)
  }

  /// build a column override matched by its natural (auto-detected) key rather than
  /// by position, e.g. to rename and/or reformat a single field out of many without
  /// needing to enumerate every column ahead of it.
  pub fn from_source_key_with_format(source_key: &str, key_opt: Option<&str>, format: Format, default: Option<Value>, datetime_mode: DateTimeMode, decimal_comma: bool) -> Self {
    let mut col = Self::from_key_ref_with_format(key_opt, format, default, datetime_mode, decimal_comma);
    col.source_key = Some(Arc::from(source_key));
    col
  }

  /// build new column data type override and optional default
  pub fn from_json(json: &Value) -> Self {
    let key_opt = json.get("key").map(|v| v.as_str().unwrap_or(""));
    let source_key = json.get("source_key").and_then(|v| v.as_str()).filter(|s| !s.is_empty());
    let fmt = match json.get("format").and_then(|v| v.as_str()) {
      Some(fmt_str) => {
        match Format::from_str(fmt_str) {
          Ok(fmt) => fmt,
          Err(_) => Format::Auto
        }
      },
      None => Format::Auto
    };
    let default = match json.get("default") {
      Some(def_val) => {
        match def_val {
          Value::String(s) => Some(Value::String(s.clone())),
          Value::Number(n) => Some(Value::Number(n.clone())),
          Value::Bool(b) => Some(Value::Bool(*b)),
          _ => None
        }
      },
      None => None
    };
    let datetime_mode = datetime_mode_from_json(json);
    let dec_commas_keys = ["decimal_comma", "dec_comma"];
    let mut decimal_comma = false;

    for key in &dec_commas_keys {
      if let Some(euro_val) = json.get(*key) {
        decimal_comma = euro_val.as_bool().unwrap_or(false);
        break;
      }
    }
    if let Some(src) = source_key {
      Column::from_source_key_with_format(src, key_opt, fmt, default, datetime_mode, decimal_comma)
    } else {
      Column::from_key_ref_with_format(key_opt, fmt, default, datetime_mode, decimal_comma)
    }
}


  // future development with column options
  #[allow(dead_code)]
  pub fn set_format(mut self, fmt: Format) -> Self {
    self.format = fmt;
    self
  }

  #[allow(dead_code)]
  pub fn set_default(mut self, val: Value) -> Self {
    self.default = Some(val);
    self
  }

  #[allow(dead_code)]
  pub fn set_datetime_mode(mut self, val: DateTimeMode) -> Self {
    self.datetime_mode = val;
    self
  }

  #[allow(dead_code)]
  pub fn set_decimal_comma(mut self, val: bool) -> Self {
    self.decimal_comma = val;
    self
  }

  pub fn from_key_ref_with_format(key_opt: Option<&str>, format: Format, default: Option<Value>, datetime_mode: DateTimeMode, decimal_comma: bool) -> Self {
    let mut key = None;
    if let Some(k_str) = key_opt {
      key = Some(Arc::from(k_str));
    }
    Column {
      key,
      source_key: None,
      format,
      default,
      datetime_mode,
      decimal_comma
    }
  }

  pub fn key_name(&self) -> String {
    self.key.clone().unwrap_or(Arc::from("")).to_string()
  }

  pub fn source_key_name(&self) -> String {
    self.source_key.clone().unwrap_or(Arc::from("")).to_string()
  }

  pub fn to_json(&self) -> Value {
    json!({
      "key": self.key_name(),
      "source_key": self.source_key_name(),
      "format": self.format.to_string(),
      "default": self.default,
      "datetime_mode": self.datetime_mode.to_string(),
      "decimal_comma": self.decimal_comma
    })
  }

  pub fn to_line(&self) -> String {
    let datetime_mode_str = if self.datetime_mode != DateTimeMode::Full {
      format!(", {}", self.datetime_mode)
    } else {
      "".to_string()
    };
    let def_string = if let Some(def_val) = self.default.clone() {
      format!("default: {}", def_val)
    } else {
      "".to_string()
    };
    let comma_str = if self.decimal_comma {
      ", decimal comma"
    } else {
      ""
    };
    let source_str = if self.source_key.is_some() {
      format!(", matched from {}", self.source_key_name())
    } else {
      "".to_string()
    };
    format!(
      "\tkey {}, format {}{}{}{}{}",
      self.key_name(),
      self.format,
      def_string,
      datetime_mode_str,
      comma_str,
      source_str)
  }

}


/// Match on permitted file types identified by file extensions
/// Unmatched means do not process
#[derive(Debug, Clone, Copy)]
pub enum Extension {
  Unmatched,
  Ods,
  Xlsx,
  Xlsm,
  Xlsb,
  Xls,
  Csv,
  Tsv,
}

impl Extension {
  pub fn from_path(path:&Path) -> Extension {
    if let Some(ext) = path.extension() {
      if let Some(ext_str) = ext.to_str() {
        let ext_lc = ext_str.to_lowercase();
        return match  ext_lc.as_str() {
          "ods" => Extension::Ods,
          "xlsx" => Extension::Xlsx,
          // .xlsm (macro-enabled) is the same OOXML container as .xlsx -- calamine's own
          // open_workbook_auto already routes both through its Xlsx reader (it does its
          // own extension check on the same path), so there's nothing macro-specific to
          // handle here; we just need to stop rejecting the extension before it gets there.
          "xlsm" => Extension::Xlsm,
          "xlsb" => Extension::Xlsb,
          "xls" => Extension::Xls,
          "csv" => Extension::Csv,
          "tsv" => Extension::Tsv,
          _ => Extension::Unmatched
        }
      }
    }
    Extension::Unmatched
  }

  /// use the Calamine library
  pub fn use_calamine(&self) -> bool {
    matches!(self, Self::Ods | Self::Xlsx | Self::Xlsm | Self::Xlsb | Self::Xls)
  }

  /// added for future development
  /// Process a simple CSV or TSV
  #[allow(dead_code)]
  pub fn use_csv(&self) -> bool {
    matches!(self, Self::Csv | Self::Tsv)
  }

}

impl std::fmt::Display for Extension {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let result = match self {
      Self::Ods => "ods",
      Self::Xlsx => "xlsx",
      Self::Xlsm => "xlsm",
      Self::Xlsb => "xlsb",
      Self::Xls => "xls",
      Self::Csv => "csv",
      Self::Tsv => "tsv",
      _ => ""
    };
    write!(f, "{}", result)
  }
}

pub struct PathData<'a> {
  path: &'a Path,
  ext: Extension
}

impl<'a> PathData<'a> {
  pub fn new(path: &'a Path) -> Self {
    PathData {
      path,
      ext: Extension::from_path(path)
    }
  }

  pub fn mode(&self) -> Extension {
    self.ext
  }

  pub fn extension(&self) -> String {
    self.ext.to_string()
  }

  pub fn ext(&self) -> Extension {
    self.ext
  }

  pub fn path(&self) -> &Path {
    self.path
  }

  pub fn is_valid(&self) -> bool {
    !matches!(self.ext, Extension::Unmatched)
  }

  pub fn use_calamine(&self) -> bool {
    self.ext.use_calamine()
  }

  pub fn filename(&self) -> String {
    if let Some(file_ref) = self.path.file_name() {
        file_ref.to_string_lossy().to_string()
    } else {
        "".to_owned()
    }
  }
}



#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadMode {
  #[default]
  Sync,
  PreviewMultiple,
  Async
}

/// either Preview or Async mode
impl ReadMode {

  pub fn from_key(key: &str) -> Self {
    let sample = key.to_lowercase().strip_non_alphanum();
    match sample.as_str() {
      "async" | "defer" | "deferred" | "a" => ReadMode::Async,
      "preview" | "p" | "pre" | "multimode" | "multiple" | "previewmultiple" | "previewmulti" | "m" => ReadMode::PreviewMultiple,
      _ => ReadMode::Sync
    }
  }

  pub fn is_async(&self) -> bool {
    matches!(self, Self::Async)
  }

  /// not preview or sync mode
  pub fn is_multimode(&self) -> bool {
    matches!(self, Self::PreviewMultiple)
  }
}

impl std::fmt::Display for ReadMode {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let result = match self {
      Self::Async => "deferred",
      Self::PreviewMultiple => "preview",
      _ => "direct"
    };
    write!(f, "{}", result)
  }
}

/// defines the column key naming convention
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FieldNameMode {
  #[default]
  AutoA1, // will use A1 column keys if headers are unavailable
  AutoNumPadded, // will use C01 format if column headers are unavailable
  A1, // Defaults to A1 columns unless custom keys are added
  NumPadded, // Defaults to C01 format unless custom keys are added
}

/// either Preview or Async mode
impl FieldNameMode {


  pub fn from_key(system: &str, override_header: bool) -> Self {
    if system.starts_with_ci("a1") {
      if override_header {
        FieldNameMode::A1
      } else {
        FieldNameMode::AutoA1
      }
    } else if system.starts_with_ci("c") || system.starts_with_ci("n") {
      if override_header {
        FieldNameMode::NumPadded
      } else {
        FieldNameMode::AutoNumPadded
      }
    } else {
      FieldNameMode::AutoA1
    }
  }


  /// use AQ column field style
  pub fn use_a1(&self) -> bool {
    matches!(self, Self::AutoA1 | Self::A1)
  }

  /// use c01 column field style
  pub fn use_c01(&self) -> bool {
    matches!(self, Self::AutoNumPadded | Self::NumPadded)
  }

   /// use seqquential a1 or C01 column style unless custom overrides are added
   pub fn override_headers(&self) -> bool {
    matches!(self, Self::NumPadded | Self::A1)
  }

  /// use default headers if available unless override by custom headers
  pub fn keep_headers(&self) -> bool {
    !self.override_headers()
  }

  /// The always-fallback variant of this style -- A1 letters or C01 numbers regardless
  /// of whether real header text is available. Used when no row should ever be treated
  /// as a source of header text at all (e.g. `omit_header`), as opposed to the "Auto"
  /// variants' normal behavior of falling back only when text happens to be missing.
  pub fn forced_fallback(&self) -> Self {
    if self.use_c01() {
      Self::NumPadded
    } else {
      Self::A1
    }
  }
}

impl std::fmt::Display for FieldNameMode {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let result = match self {
      Self::AutoNumPadded => "C01 auto",
      Self::NumPadded => "C01 override",
      Self::A1 => "A1 override",
      _ => "A1 auto",
    };
    write!(f, "{}", result)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_format_mode() {
    let custom_boolean = Format::truthy_custom("si", "no");
    assert_eq!(custom_boolean.to_string(), "truthy(si,no)");
  }

  #[test]
  fn test_match_truthy_custom() {
    let (true_keys, false_keys) = match_custom_truthy("tr:si,no").unwrap();
    assert_eq!("si", true_keys);
    assert_eq!("no", false_keys);
  }

  #[test]
  fn test_first_data_row_index_defaults_to_none_when_both_unset() {
    // No additional gating when neither header_row nor data_row_index is set -- callers
    // should skip the check entirely rather than compute a value that changes nothing.
    let opts = OptionSet::new("x.xlsx");
    assert_eq!(opts.first_data_row_index(), None);
  }

  #[test]
  fn test_first_data_row_index_defaults_to_right_after_header_row() {
    // header_row=2 (0-based); with no explicit data_row_index, data starts immediately
    // after, at 0-based row 3.
    let opts = OptionSet::new("x.xlsx").header_row(2);
    assert_eq!(opts.first_data_row_index(), Some(3));
  }

  #[test]
  fn test_first_data_row_index_honors_explicit_gap() {
    // header_row=2, data_row_index=4 (both 0-based) -- row 3 (the row directly below the
    // header) is a gap that gets skipped.
    let opts = OptionSet::new("x.xlsx").header_row(2).data_row_index(4);
    assert_eq!(opts.first_data_row_index(), Some(4));
  }

  #[test]
  fn test_first_data_row_index_honors_data_row_equal_to_header() {
    // data_row_index equal to the header row is a legitimate (if rare) configuration --
    // e.g. a CSV with predefined/external headers where no line is actually consumed as
    // a header -- so it's honored literally, not silently bumped to header_row + 1.
    let opts = OptionSet::new("x.xlsx").header_row(2).data_row_index(2);
    assert_eq!(opts.first_data_row_index(), Some(2));
  }

  #[test]
  fn test_first_data_row_index_ignores_data_row_before_header() {
    // data_row_index set strictly *before* the header row is nonsensical -- falls back
    // to "immediately after the header row" instead of producing zero data rows.
    let opts = OptionSet::new("x.xlsx").header_row(2).data_row_index(0);
    assert_eq!(opts.first_data_row_index(), Some(3));
  }

  #[test]
  fn test_first_data_row_index_with_only_data_row_set() {
    // header_row unset (defaults to 0); data_row_index=1 skips over row 0 (the header)
    // even though header_row itself was never explicitly set.
    let opts = OptionSet::new("x.xlsx").data_row_index(1);
    assert_eq!(opts.first_data_row_index(), Some(1));
  }

  #[test]
  fn test_xlsm_is_recognised_and_routed_through_calamine() {
    // Regression: .xlsm (macro-enabled) is the same OOXML container as .xlsx -- calamine's
    // own open_workbook_auto already reads both through its Xlsx reader -- but our own
    // Extension enum didn't recognise the extension at all, so .xlsm files were rejected
    // before calamine ever got a chance to open them.
    let ext = Extension::from_path(Path::new("workbook.xlsm"));
    assert!(matches!(ext, Extension::Xlsm));
    assert!(ext.use_calamine());
    assert_eq!(ext.to_string(), "xlsm");
  }

}