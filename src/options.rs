use serde_json::{json, Error, Value};
use simple_string_patterns::SimpleMatch;
use crate::headers::*;
use std::{path::Path, str::FromStr, sync::Arc};
/// default max number of rows without an override via ->max_row_count(max_row_count)
pub const DEFAULT_MAX_ROWS: usize = 10_000;

/// Row parsing options with nested column options
#[derive(Debug, Clone, Default)]
pub struct RowOptionSet {
  pub euro_number_format: bool, // always parse as euro number format
  pub date_only: bool,
  pub columns: Vec<Column>,
}

impl RowOptionSet {
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
  pub sheet: Option<String>, // Optional sheet name reference. Will default to index value if not matched
  pub index: u32, // worksheet index
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
      sheet: None,
      index: 0,
      path: Some(path_str.to_string()),
      rows: RowOptionSet::default(),
      jsonl: false,
      max: None,
      omit_header: false,
      header_row: 0,
      read_mode: ReadMode::Sync,
      field_mode: FieldNameMode::AutoA1
    }
}


  /// Sets the sheet name for the operation.
  pub fn sheet_name(&mut self, name: String) -> &mut Self {
      self.sheet = Some(name);
      self
  }

  /// Sets the sheet index.
  pub fn sheet_index(&mut self, index: u32) -> &mut Self {
      self.index = index;
      self
  }
  /// Sets JSON Lines mode to true.
  pub fn json_lines(&mut self) -> &mut Self {
      self.jsonl = true;
      self
  }

  /// Omits the header when reading.
  pub fn omit_header(&mut self) -> &mut Self {
      self.omit_header = true;
      self
  }

  /// Sets the header row index
  pub fn header_row(&mut self, row: u8) -> &mut Self {
      self.header_row = row;
      self
  }

  /// Sets the maximum number of rows to read.
  pub fn max_row_count(&mut self, max: u32) -> &mut Self {
      self.max = Some(max);
      self
  }

  /// Sets the read mode to asynchronous.
  pub fn read_mode_async(&mut self) -> &mut Self {
      self.read_mode = ReadMode::Async;
      self
  }

  /// Sets the column key naming convetion
  pub fn field_name_mode(&mut self, system: &str, override_header: bool) -> &mut Self {
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
    json!({
      "sheet": {
        "key": self.sheet.clone().unwrap_or("".to_string()),
        "index": self.index,
      },
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

  pub fn to_lines(&self) -> Vec<String> {

    let mut lines = vec![];
    if let Some(s_name) = self.sheet.clone() {
      lines.push(format!("sheet name: {}", s_name));
    } else if self.index > 0 {
      lines.push(format!("sheet index: {}", self.index));
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
      format!("date mode: {}", self.rows.date_mode())
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

  /// set the maximum of rows to be output synchronously
  pub fn max_rows(&self) -> usize {
    if self.read_mode == ReadMode::PreviewAsync {
      return 20
    }
    if let Some(mr) = self.max {
      mr as usize
    } else {
      DEFAULT_MAX_ROWS
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
      Self::Truthy => "truthy",
      Self::TruthyCustom(yes, no) => &format!("truthy({},{})", yes, no),
    };
    result.to_string() 
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
        _ => Self::Auto,
      };
      Ok(fmt)
  }
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
      Self::Ods | Self::Xlsx | Self::Xls => true,
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
  PreviewAsync,
  Async
}

/// either Preview or Async mode
impl ReadMode {
  pub fn is_async(&self) -> bool {
    match self {
      Self::Sync => false,
      _ => true
    }
  }

  /// not preview or sync mode
  pub fn is_full_async(&self) -> bool {
    match self {
      Self::Async => true,
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
      FieldNameMode::A1
    }
  }

  pub fn use_a1(&self) -> bool {
    match self {
      Self::AutoA1 | Self::A1 => true,
      _ => false
    }
  }

  /// not preview or sync mode
  pub fn use_c01(&self) -> bool {
    match self {
      Self::AutoNumPadded | Self::NumPadded => true,
      _ => false
    }
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