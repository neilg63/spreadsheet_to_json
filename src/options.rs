use clap::Parser;

/// Command line arguments configuration
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
  
  #[clap(short, long, value_parser) ]
  pub sheet_ref: Option<String>,

  #[clap(short, long, value_parser, default_value_t = 0)]
  pub sheet_index: u32,
  
  pub path: Option<String>,

  #[clap(short, long, value_parser, default_value_t = false)]
  pub enforce_euro_number_format: bool,

  #[clap(short, long, value_parser, default_value_t = false) ]
  pub date_only: bool,

  #[clap(short = 'h', long, value_parser) ]
  pub header_list: Option<String>,

  #[clap(short, long, value_parser) ]
  pub max_rows: Option<u32>,

  #[clap(short = 't', long, value_parser, default_value_t = 0) ]
  pub header_row: u8,

  #[clap(short = 'o',long, value_parser, default_value_t = false) ]
  pub omit_header: bool,

}