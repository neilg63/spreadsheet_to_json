mod options;
mod headers;
mod args;
mod data_set;
mod reader;
mod euro_number_format;
mod is_truthy;

use std::env;

use clap::Parser;
use args::*;
use serde_json::json;

use options::*;
use reader::*;


fn main() {
    let args = Args::parse();
    let opts = OptionSet::from_args(&args);
    env::set_var("RUST_BACKTRACE", "1");
    
   let result = render_spreadsheet(&opts);
   let json_value = match result {
    Err(msg) => json!{ { "error": true, "message": msg.to_string(), "options": opts.to_json() } },
    Ok(data_set) => {
        if args.exclude_cells {
            json!({
                "options": opts.to_json() 
            })
        } else {
            data_set.to_json()
        }
    }
   };

   println!("{}", json_value);

}
