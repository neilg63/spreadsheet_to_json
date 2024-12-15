mod options;
mod headers;
mod args;
mod data_set;
mod reader;
mod euro_number_format;
mod is_truthy;

use std::env;
use tokio;
use clap::{error::Error, Parser};
use args::*;
use serde_json::json;

use options::*;
use reader::*;


#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let opts = OptionSet::from_args(&args);
    if args.debug {
        env::set_var("RUST_BACKTRACE", "1");
    }
    let mut output_lines = false;
    let mut lines: Option<String> = None;
    let result = render_spreadsheet_direct(&opts).await;
    let json_value = match result {
        Err(msg) => json!{ { "error": true, "message": msg.to_string(), "options": opts.to_json() } },
        Ok(data_set) => {
            output_lines = args.jsonl;
            if output_lines {
                lines = Some(data_set.rows().join("\n"));
            }
            if args.exclude_cells {
                json!({
                    "options": opts.to_json() 
                })
            } else {
                data_set.to_json()
            }
        }
    };
    if output_lines {
        if let Some(lines_string) = lines {
            println!("{}", lines_string);
        }
    } else {
        println!("{}", json_value);
    }
    Ok(())
}
