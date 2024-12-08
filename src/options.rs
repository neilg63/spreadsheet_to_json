use crate::headers::*;
use std::sync::Arc;


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

#[derive(Debug, Clone)]
pub struct Column {
  pub key:  Arc<str>,
  pub format: Format,
  pub date_only: bool, // date only in Format::Auto mode with datetime objects
  pub euro_number_format: bool, // parse as euro number format
  pub default: Option<serde_json::Value>,
}

impl Column {


  pub fn from_key_index(key_opt: Option<&str>, index: usize) -> Self {
    Self::from_key_ref_with_format(key_opt, index, Format::Auto, None, false, false)
  }

  pub fn from_key_custom(key_opt: Option<&str>, index: usize, date_only: bool, euro_number_format: bool) -> Self {
    Self::from_key_ref_with_format(key_opt, index, Format::Auto, None, date_only, euro_number_format)
  }

  pub fn from_key_ref_with_format(key_opt: Option<&str>, index: usize, format: Format, default: Option<serde_json::Value>, date_only: bool, euro_number_format: bool) -> Self {
    let key = key_opt.map(Arc::from).unwrap_or_else(|| Arc::from(to_head_key(index)));
    Column {
      key,
      format,
      default,
      date_only,
      euro_number_format
    }
  }

}
