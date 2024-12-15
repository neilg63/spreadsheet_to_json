use clap::Error;
use serde_json::{json, Value};
use crate::headers::*;
use std::{path::Path, str::FromStr, sync::Arc};



#[derive(Debug, Clone)]
pub struct RowOptionSet {
  pub euro_number_format: bool, // always parse as euro number format
  pub date_only: bool,
  pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct OptionSet {
  pub sheet: Option<String>, // Optional sheet name reference. Will default to index value if not matched
  pub index: u32, // worksheet index
  pub path: Option<String>, // path argument. If None, do not attempt to parse
  pub rows: RowOptionSet,
  pub max: Option<u32>,
  pub omit_header: bool,
  pub header_row: u8,
  pub read_mode: ReadMode,
}

impl RowOptionSet {
  pub fn column(&self, index: usize) -> Option<&Column> {
    self.columns.get(index)
  }
}

impl OptionSet {
  pub fn to_json(&self) -> Value {
    json!({
      "sheet": {
        "key": self.sheet.clone().unwrap_or("".to_string()),
        "index": self.index,
      },
      "path": self.path.clone().unwrap_or("".to_string()),
      "euro_number_format": self.rows.euro_number_format,
      "date_only": self.rows.date_only,
      "columns": self.rows.columns.clone().into_iter().map(|c| c.to_json()).collect::<Vec<Value>>(),
      "max": self.max.unwrap_or(0),
      "header_row": self.header_row,
      "omit_header": self.omit_header,
    })
  }

  

  pub fn header_row_index(&self) -> usize {
    self.header_row as usize
  }

  pub fn max_rows(&self) -> usize {
    if self.read_mode == ReadMode::PreviewAsync {
      return 20
    }
    if let Some(mr) = self.max {
      mr as usize
    } else {
      default_max_rows()
    }
  }

  pub fn columns(&self) -> Vec<Column> {
    self.rows.columns.clone()
  }

  pub fn read_mode(&self) -> ReadMode {
    self.read_mode.clone()
  }

  pub fn capture_rows(&self) -> bool {
    match self.read_mode {
      ReadMode::Async => false,
      _ => true
    }
  }
}


#[derive(Debug, Clone)]
pub enum Format {
  Auto,
  Text,
  Integer,
  Decimal(u8),
  Boolean,
  Date,
  DateTime,
  Truthy,
  TruthyCustom(Arc<str>, Arc<str>)
}

impl ToString for Format {
  fn to_string(&self) -> String {
      match self {
        Self::Auto => "auto",
        Self::Text => "text",
        Self::Integer => "integer",
        Self::Decimal(n) => "decimal($n)",
        Self::Boolean => "boolean",
        Self::Date => "date",
        Self::DateTime => "datetime",
        Self::Truthy => "truthy",
        Self::TruthyCustom(yes, no) => "truthy_custom($yes,$no)",
      }.to_string()
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
        "t" | "truthy" => Self::Truthy,
        _ => Self::Auto,
      };
      Ok(fmt)
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


  pub fn from_key_index(key_opt: Option<&str>, index: usize) -> Self {
    Self::from_key_ref_with_format(key_opt, index, Format::Auto, None, false, false)
  }

  pub fn from_key_custom(key_opt: Option<&str>, index: usize, date_only: bool, euro_number_format: bool) -> Self {
    Self::from_key_ref_with_format(key_opt, index, Format::Auto, None, date_only, euro_number_format)
  }

  pub fn from_key_ref_with_format(key_opt: Option<&str>, index: usize, format: Format, default: Option<Value>, date_only: bool, euro_number_format: bool) -> Self {
    let key = key_opt.map(Arc::from).unwrap_or_else(|| Arc::from(to_head_key(index)));
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
      "date_only": self.date_only,
      "euro_number_format": self.euro_number_format,
      "default": self.default
    })
  }

}



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

  fn use_calamine(&self) -> bool {
    match self {
      Self::Ods | Self::Xlsx | Self::Xls => true,
      _ => false
    }
  }

  fn use_csv(&self) -> bool {
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



#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadMode {
  Sync,
  PreviewAsync,
  Async
}


fn default_max_rows() -> usize {
  dotenv::var("DEFAULT_MAX_ROWS")
  .ok()
  .and_then(|s| s.parse::<usize>().ok())
  .unwrap_or(10000)
}