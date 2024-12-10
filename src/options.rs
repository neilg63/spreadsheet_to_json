use clap::Error;
use serde_json::{json, Value};

use crate::headers::*;
use std::{str::FromStr, sync::Arc};


#[derive(Debug, Clone)]
pub struct OptionSet {
  pub sheet: Option<String>, // Optional sheet name reference. Will default to index value if not matched
  pub index: u32, // worksheet index
  pub path: Option<String>, // path argument. If None, do not attempt to parse
  pub euro_number_format: bool, // always parse as euro number format
  pub date_only: bool,
  pub columns: Vec<Column>,
  pub max: Option<u32>,
  pub header_row: u8,
  pub omit_header: bool,

}

impl OptionSet {
  pub fn to_json(&self) -> Value {
    json!({
      "sheet": {
        "key": self.sheet.clone().unwrap_or("".to_string()),
        "index": self.index,
      },
      "path": self.path.clone().unwrap_or("".to_string()),
      "euro_number_format": self.euro_number_format,
      "date_only": self.date_only,
      "columns": self.columns.clone().into_iter().map(|c| c.to_json()).collect::<Vec<Value>>(),
      "max": self.max.unwrap_or(0),
      "header_row": self.header_row,
      "omit_header": self.omit_header
    })
  }

  pub fn header_row_index(&self) -> usize {
    self.header_row as usize
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
  DateTime
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
        Self::DateTime => "datetime"
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
