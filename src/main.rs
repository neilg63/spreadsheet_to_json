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
    Err(msg) => json!{ { "error": true, "message": msg.to_string() } }.to_string(),
    Ok(data_set) => data_set.to_json()
    //Ok(data_set) => json!({"ok": true}).to_string()
   };

   println!("{}", json_value);

}
