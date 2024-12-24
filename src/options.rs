use heck::ToSnakeCase;
use serde_json::{json, Error, Value};
use simple_string_patterns::{SimpleMatch, StripCharacters, ToSegments};
use crate::headers::*;
use std::{path::Path, str::FromStr, sync::Arc};
/// default max number of rows in direct single sheet mode without an override via ->max_row_count(max_row_count)
pub const DEFAULT_MAX_ROWS: usize = 10_000;
/// default max number of rows multiple sheet preview mode without an override via ->max_row_count(max_row_count)
pub const DEFAULT_MAX_ROWS_PREVIEW: usize = 1000;

/// Row parsing options with nested column options
#[derive(Debug, Clone, Default)]
pub struct RowOptionSet {
  pub columns: Vec<Column>,
  pub euro_number_format: bool, // always parse as euro number format
  pub date_only: bool,
}

impl RowOptionSet {

  // simple constructor with column keys only
  pub fn simple(cols: &[Column]) -> Self {
    RowOptionSet {
      euro_number_format: false,
      date_only: false,
      columns: cols.to_vec()
    }
  }

  // lets you set all options
  pub fn new(cols: &[Column], decimal_comma: bool, date_only: bool) -> Self {
    RowOptionSet {
      euro_number_format: decimal_comma,
      date_only,
      columns: cols.to_vec()
    }
  }

  pub fn column(&self, index: usize) -> Option<&Column> {
    self.columns.get(index)
  }

  pub fn date_mode(&self) -> String {
    if self.date_only {
      "date only"
    } else {
      "date/time"
    }.to_string()
  }

  pub fn decimal_separator(&self) -> String {
    if self.euro_number_format {
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
  pub header_row: u8,
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
        header_row: 0,
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

  /// Sets the header row index.
  pub fn header_row(mut self, row: u8) -> Self {
      self.header_row = row;
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

  /// Override matched and unmatched headers with custom headers.
  pub fn override_headers(mut self, keys: &[&str]) -> Self {
    let mut columns: Vec<Column> = Vec::with_capacity(keys.len());
    let mut index = 0;
    for ck in keys {
        columns.push(Column::from_key_index(Some(&ck.to_snake_case()), index));
        index += 1;
    }
    self.rows = RowOptionSet::simple(&columns);
    self
  }

  /// Override matched and unmatched columns with custom keys and/or formatting options
  pub fn override_columns(mut self, cols: &[Value]) -> Self {
    let mut columns: Vec<Column> = Vec::with_capacity(cols.len());
    let mut index = 0;
    for ck in cols {
        let key = ck.get("key").unwrap().as_str().unwrap();
        let fmt = match ck.get("format") {
          Some(fmt_val) => {
            match Format::from_str(fmt_val.as_str().unwrap()) {
              Ok(fmt) => fmt,
              Err(_) => Format::Auto
            }
          },
          None => Format::Auto
        };
        let default = match ck.get("default") {
          Some(def_val) => {
            match def_val {
              Value::String(s) => Some(Value::String(s.clone())),
              Value::Number(n) => Some(Value::Number(n.clone())),
              Value::Bool(b) => Some(Value::Bool(b.clone())),
              _ => None
            }
          },
          None => None
        };
        let date_only = match ck.get("date_only") {
          Some(date_val) => date_val.as_bool().unwrap_or(false),
          None => false
        };
        let dec_commas_keys = ["euro_number_format", "decimal_comma"];
        let mut euro_number_format = false;

        for key in &dec_commas_keys {
            if let Some(euro_val) = ck.get(*key) {
                euro_number_format = euro_val.as_bool().unwrap_or(false);
                break;
            }
        }
        columns.push(Column::from_key_ref_with_format(Some(key), index,fmt, default, date_only, euro_number_format));
        index += 1;
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
      "json lines"
    } else {
      ""
    }.to_string()
  }

  pub fn header_mode(&self) -> String {
    if self.jsonl {
      "json lines"
    } else {
      ""
    }.to_string()
  }

  pub fn to_json(&self) -> Value {
    let selected = if self.multimode() {
      json!({
        "sheets": self.selected.clone().unwrap_or(vec![]),
        "indices": self.indices.clone()
      })
    } else {
      json!({
        "sheet": self.selected.clone().unwrap_or(vec![]),
        "index": self.indices.get(0).unwrap_or(&0)
      })
    };
    json!({
      "selected": selected,
      "path": self.path.clone().unwrap_or("".to_string()),
      "decimal_separator": self.rows.decimal_separator(),
      "date_only": self.rows.date_only,
      "columns": self.rows.columns.clone().into_iter().map(|c| c.to_json()).collect::<Vec<Value>>(),
      "max": self.max.unwrap_or(0),
      "header_row": self.header_row,
      "omit_header": self.omit_header,
      "jsonl": self.jsonl
    })
  }

  pub fn index_list(&self) -> String {
    self.indices.clone().into_iter().map(|s| s.to_string()).collect::<Vec<String>>().join(", ")
  }

  pub fn to_lines(&self) -> Vec<String> {
    let mut lines = vec![];
    if let Some(s_names) = self.selected.clone() {
      let plural = if s_names.len() > 1 {
        "s"
      } else {
        ""
      };
      lines.push(format!("sheet name{}: {}", plural, s_names.join(",")));
    } else if self.indices.len() > 0 {
      lines.push(format!("sheet indices: {}", self.index_list()));
    }
    if let Some(path) = self.path.clone() {
      lines.push(format!("path: {}", path));
    }
    lines.extend(vec![
      format!("max: {}", self.max.unwrap_or(0) ),
      format!("header row: {}", self.header_row),
      format!("headers: {}", self.header_mode()),
      format!("mode: {}", self.row_mode()),
      format!("decimal separator: {}", self.rows.decimal_separator()),
      format!("date mode: {}", self.rows.date_mode()),
      format!("column style: {}", self.field_mode.to_string())
    ]);

    if self.columns().len() > 0 {
      lines.push("columns:".to_string());
      for col in self.rows.columns.clone() {
        lines.push(col.to_line());
      }
    }
    lines
  }

  /// header row index as usize
  pub fn header_row_index(&self) -> usize {
    self.header_row as usize
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
    self.read_mode.clone()
  }

  /// Needs full data set to processed later
  pub fn is_async(&self) -> bool {
    self.read_mode.is_async()
  }

  // Should rows be captured synchronously
  pub fn capture_rows(&self) -> bool {
    match self.read_mode {
      ReadMode::Async => false,
      _ => true
    }
  }

}


/// Cell format overrides
#[derive(Debug, Clone)]
pub enum Format {
  Auto, // automatic interpretation
  Text, // text
  Integer, // integer only
  Decimal(u8), // decimal to stated precision
  Boolean, // Boolean or  cast to boolean from integers
  Date, // Interpret as date only
  DateTime, // Interpret as full datetime
  DateTimeCustom(Arc<str>),
  Truthy, // interpret common yes/no, y/n, true/false text strings as true/false
  #[allow(dead_code)]
  TruthyCustom(Arc<str>, Arc<str>) // define custom yes/no values
}

impl ToString for Format {
  fn to_string(&self) -> String {
    let result = match self {
      Self::Auto => "auto",
      Self::Text => "text",
      Self::Integer => "integer",
      Self::Decimal(n) => &format!("decimal({})", n),
      Self::Boolean => "boolean",
      Self::Date => "date",
      Self::DateTime => "datetime",
      Self::DateTimeCustom(fmt) => &format!("datetime({})", fmt),
      Self::Truthy => "truthy",
      Self::TruthyCustom(yes, no) => &format!("truthy({},{})", yes, no),
    };
    result.to_string() // Convert the string slice to a String
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
        "b" | "bool" | "boolean" => Self::Boolean,
        "da" | "date" => Self::Date,
        "dt" | "datetime" => Self::DateTime,
        "tr" | "truthy" => Self::Truthy,
        _ => {
          if let Some(str) = match_custom_dt(key) {
            Self::DateTimeCustom(Arc::from(str))
          } else if let Some((yes, no)) = match_custom_truthy(key) {
            Self::TruthyCustom(Arc::from(yes), Arc::from(no))
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
  let (head, tail) = test_str.to_head_tail(":");
  if tail.len() > 1 && head.len() > 1 && head.starts_with_ci("tr") {
    let (yes, no) = tail.to_head_tail(",");
    if yes.len() > 0 && no.len() > 0 {
      return Some((yes, no));
    }
  }
  None
}

impl Format {
  #[allow(dead_code)]
  pub fn truthy_custom(yes: &str, no: &str) -> Self {
    Format::TruthyCustom(Arc::from(yes), Arc::from(no))
  }
}

#[derive(Debug, Clone)]
pub struct Column {
  pub key:  Arc<str>,
  pub format: Format,
  pub default: Option<Value>,
  pub date_only: bool, // date only in Format::Auto mode with datetime objects
  pub euro_number_format: bool, // parse as euro number format
}

impl Column {

  /// build from core options and sheet index only
  pub fn from_key_index(key_opt: Option<&str>, index: usize) -> Self {
    Self::from_key_ref_with_format(key_opt, index, Format::Auto, None, false, false)
  }

  // future development with column options
  #[allow(dead_code)]
  pub fn from_key_custom(key_opt: Option<&str>, index: usize, date_only: bool, euro_number_format: bool) -> Self {
    Self::from_key_ref_with_format(key_opt, index, Format::Auto, None, date_only, euro_number_format)
  }

  pub fn from_key_ref_with_format(key_opt: Option<&str>, index: usize, format: Format, default: Option<Value>, date_only: bool, euro_number_format: bool) -> Self {
    let key = key_opt.map(Arc::from).unwrap_or_else(|| Arc::from(to_head_key_default(index)));
    Column {
      key,
      format,
      default,
      date_only,
      euro_number_format
    }
  }

  pub fn to_json(&self) -> Value {
    json!({
      "key": self.key.to_string(),
      "format": self.format.to_string(),
      "default": self.default,
      "date_only": self.date_only,
      "euro_number_format": self.euro_number_format
    })
  }

  pub fn to_line(&self) -> String {
    let date_only_str = if self.date_only {
      ", date only"
    } else {
      ""
    }.to_owned();
    let def_string = if let Some(def_val) = self.default.clone() {
      format!("default: {}", def_val.to_string())
    } else {
      "".to_string()
    };
    let comma_str = if self.euro_number_format {
      ", decimal comma"
    } else {
      ""
    };
    format!(
      "\tkey {}, format {}{}{}{}",
      self.key.to_string(),
      self.format.to_string(),
      def_string,
      date_only_str,
      comma_str)
  }

}


/// Match on permitted file types identified by file extensions
/// Unmatched means do not process
#[derive(Debug, Clone, Copy)]
pub enum Extension {
  Unmatched,
  Ods,
  Xlsx,
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
    match self {
      Self::Ods | Self::Xlsx | Self::Xlsb | Self::Xls => true,
      _ => false
    }
  }
  
  /// added for future development
  /// Process a simple CSV or TSV
  #[allow(dead_code)]
  pub fn use_csv(&self) -> bool {
    match self {
      Self::Csv | Self::Tsv => true,
      _ => false
    }
  }

}

impl ToString for Extension {
  fn to_string(&self) -> String {
    match self {
      Self::Ods => "ods",
      Self::Xlsx => "xlsx",
      Self::Xlsb => "xlsb",
      Self::Xls => "xls",
      Self::Csv => "csv",
      Self::Tsv => "tsv",
      _ => ""
    }.to_string()
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
    match self.ext {
      Extension::Unmatched => false,
      _ => true
    }
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
    match self {
      Self::Async => true,
      _ => false
    }
  }

  /// not preview or sync mode
  pub fn is_multimode(&self) -> bool {
    match self {
      Self::PreviewMultiple => true,
      _ => false
    }
  }
}

/// defines the column key naming convention
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FieldNameMode {
  #[default]
  AutoA1, // will use A1 column keys if headers are unavailable
  AutoNumPadded, // will use C01 format if column headers are unavailable
  A1, // Defaults to A1 columns unless custom keys are added
  NumPadded // Defaults to C01 format unless custom keys are added
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
    match self {
      Self::AutoA1 | Self::A1 => true,
      _ => false
    }
  }

  /// use c01 column field style
  pub fn use_c01(&self) -> bool {
    match self {
      Self::AutoNumPadded | Self::NumPadded => true,
      _ => false
    }
  }

   /// use seqquential a1 or C01 column style unless custom overrides are added
   pub fn override_headers(&self) -> bool {
    match self {
      Self::NumPadded | Self::A1 => true,
      _ => false
    }
  }

  /// use default headers if available unless override by custom headers
  pub fn keep_headers(&self) -> bool {
    self.override_headers() == false
  }
}

impl ToString for FieldNameMode {
  fn to_string(&self) -> String {
    match self {
      Self::AutoNumPadded => "C01 auto",
      Self::NumPadded => "C01 override",
      Self::A1 => "A1 override",
      _ => "A1 auto",
    }.to_string()    
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

}