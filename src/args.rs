use clap::Parser;
use crate::options::{OptionSet, Column};
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

}

pub trait FromArgs {
    fn from_args(args: &Args) -> Self;
}

impl FromArgs for OptionSet {
    fn from_args(args: &Args) -> Self {

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