use clap::Parser;
use crate::headers::*;
use std::borrow::Cow;
use std::sync::Arc;
use simple_string_patterns::*;

/// Command line arguments configuration
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
  
  #[clap(short, long, value_parser) ]
  pub sheet: Option<String>,

  #[clap(short, long, value_parser, default_value_t = 0)]
  pub index: u32,
  
  pub path: Option<String>,

  #[clap(long, value_parser, default_value_t = false)]
  pub euro_number_format: bool,

  #[clap(long, value_parser, default_value_t = false) ]
  pub date_only: bool,

  #[clap(short = 'k', long, value_parser) ]
  pub keys: Option<String>,

  #[clap(short, long, value_parser) ]
  pub max: Option<u32>,

  #[clap(short = 't', long, value_parser, default_value_t = 0) ]
  pub header_row: u8,

  #[clap(short = 'o',long, value_parser, default_value_t = false) ]
  pub omit_header: bool,

}

 fn to_strs<'a>(string_vec: &'a Vec<String>) -> Vec<&'a str> {
  string_vec.iter().map(String::as_str).collect()
}

#[derive(Debug, Clone)]
pub struct OptionSet {
  pub sheet: Option<String>,
  pub index: u32, // worksheet index
  pub path: Option<String>, // always parse as euro number format
  pub euro_number_format: bool, // always parse as euro number format
  pub date_only: bool,
  pub columns: Vec<Column>,
  pub max: Option<u32>,
  pub header_row: u8,
  pub omit_header: bool,

}

impl OptionSet {
  pub fn from_args(args: &Args) -> Self {

    let mut columns: Vec<Column> = vec![];
    let mut index = 0;
    if let Some(k_string) = args.keys.clone() {
      let split_parts = k_string.to_segments(".");
      for ck in split_parts {
        columns.push(Column::from_key_index(Some(&ck), index));
        index += 1;
      }
    }
    OptionSet {
      sheet: args.sheet.clone(),
      index: args.index,
      path: args.path.clone(),
      euro_number_format: args.euro_number_format,
      date_only: args.date_only,
      columns,
      max: args.max,
      header_row: args.header_row,
      omit_header: args.omit_header
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