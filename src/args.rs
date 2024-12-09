use std::str::FromStr;

use clap::Parser;
use heck::ToSnakeCase;
use serde_json::{Number, Value};
use crate::{options::{Column, OptionSet}, Format, is_truthy::*};
use simple_string_patterns::ToSegments;

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

  #[clap(short = 'x',long, value_parser, default_value_t = false) ]
  pub exclude_cells: bool, // test validity only and show options

}

pub trait FromArgs {
    fn from_args(args: &Args) -> Self;
}

impl FromArgs for OptionSet {
    fn from_args(args: &Args) -> Self {

    let mut columns: Vec<Column> = vec![];
    let mut index = 0;
    if let Some(k_string) = args.keys.clone() {
        let split_parts = k_string.to_segments(",");
        for ck in split_parts {
            let sub_parts = ck.to_segments(":");
            let num_subs = sub_parts.len();
            if num_subs < 2 {
                columns.push(Column::from_key_index(Some(&ck.to_snake_case()), index));
            } else {
                let fmt = Format::from_str(sub_parts.get(1).unwrap_or(&"auto".to_string())).unwrap_or(Format::Auto);
                let mut default_val = None;
                if let Some(def_val) = sub_parts.get(2) {
                    default_val = match fmt {
                        Format::Integer => Some(Value::Number(Number::from_i128(i128::from_str(&def_val).unwrap()).unwrap())),
                        Format::Boolean => {
                            if let Some(is_true) = is_truthy_core(def_val, false) {
                                Some(Value::Bool(is_true))
                            } else {
                                None
                            }
                        },
                        _ => Some(Value::String(def_val.clone()))
                    }
                }
                columns.push(Column::from_key_ref_with_format(Some(&ck.to_snake_case()), index, fmt, default_val, false, false));
            }
            
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