mod options;
mod headers;
mod args;
mod data_set;
mod reader;

use clap::Parser;
use args::*;
use serde_json::json;

use options::*;
use reader::*;


fn main() {
    let args = Args::parse();
    let opts = OptionSet::from_args(&args);
    
   let result = render_spreadsheet(&opts);
   let json_value = match result {
    Err(msg) => json!{ { "error": true, "message": msg.to_string(), "options": opts.to_json() } }.to_string(),
    Ok(data_set) => {
        if args.exclude_cells {
            json!({
                "options": opts.to_json() 
            }).to_string()
        } else {
            data_set.to_json()
        }
    }
   };

   println!("{}", json_value);

}
