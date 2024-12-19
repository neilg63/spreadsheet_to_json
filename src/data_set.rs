use std::{fs::File, io::BufReader, sync::Arc};
use calamine::{Reader, Sheets};
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::{json, Value};
use simple_string_patterns::*;

use crate::{OptionSet, PathData, ReadMode};


/// Core info about a spreadsheet with extension, matched worksheet name and index an all worksheet keys
#[derive(Debug, Clone)]
pub struct WorkbookInfo {
    pub filename: String,
    pub extension: String,
    pub sheet: (String, usize),
    pub sheets: Vec<String>,
}

impl WorkbookInfo {
    pub fn new(path_data: &PathData, sheet: &str, sheet_index: usize, sheet_refs: &[String]) -> Self {
        WorkbookInfo {
            extension: path_data.extension(),
            filename: path_data.filename(), 
            sheet: (sheet.to_owned(), sheet_index),
            sheets: sheet_refs.to_vec(),
            //sheets: sheet_refs.into_iter().map(|s| s.to_string()).collect(),
        }
    }

    pub fn simple(path_data: &PathData) -> Self {
        let sheet_name = "single";
        WorkbookInfo {
            extension: path_data.extension(),
            filename: path_data.filename(), 
            sheet: (sheet_name.to_owned(), 0),
            sheets: vec![sheet_name.to_owned()],
        }
    }

    pub fn ext(&self) -> String {
        self.extension.to_owned()
    }

    pub fn name(&self) -> String {
        self.filename.to_owned()
    }

    pub fn sheet(&self) -> (String, usize) {
        self.sheet.clone()
    }

    pub fn sheets(&self) -> Vec<String> {
        self.sheets.clone()
    }
}


// Result set
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub filename: String,
    pub extension: String,
    pub sheet: (String, usize),
    pub sheets: Vec<String>,
    pub keys: Vec<String>,
    pub num_rows: usize,
    pub data: Vec<IndexMap<String, Value>>,
    pub out_ref: Option<String>
}

impl ResultSet {

  /// Instantiate with Core workbook info, header keys, data set and optional output reference
  pub fn new(info: WorkbookInfo, keys: &[String], data_set: DataSet, out_ref: Option<&str>) -> Self {
    let (num_rows, data) = match data_set {
      DataSet::WithRows(size, rows) => (size, rows),
      DataSet::Count(size) => (size, vec![])
    };
    ResultSet {
      extension: info.ext(),
      filename: info.name(), 
      sheet: info.sheet(),
      sheets: info.sheets(),
      keys: keys.to_vec(),
      num_rows,
      data,
      out_ref: out_ref.map(|s| s.to_string())
    }
  }

  /// Full result set as JSON with criteria, options and data in synchronous mode
  pub fn to_json(&self) -> Value {
    let mut result = json!({
      "name": self.filename,
      "extension": self.extension,
      "sheet": {
          "key": self.sheet.0,
          "index": self.sheet.1
      },
      "sheets": self.sheets,
      "num_rows": self.num_rows,
      "fields": self.keys,
      "data": self.data,
    });
    if let Some(out_ref_str) = self.out_ref.clone() {
      result["outref"] = json!(out_ref_str);
    }
    result
  }

   /// Full result set as CLI-friendly lines
   pub fn to_output_lines(&self, json_lines: bool) -> Vec<String> {
    let mut lines = vec![
      format!("name:{}", self.filename),
      format!("extension: {}", self.extension),
      format!("sheet: name: {}, index: {}", self.sheet.0, self.sheet.1),
      format!("sheets: {}", self.sheets.join(", ")),
      format!("row count: {}", self.num_rows),
      format!("fields: {}", self.keys.join(",")),
    ];
    if let Some(out_ref_str) = self.out_ref.clone() {
      lines.push(format!("output reference: {}", out_ref_str));
    } else {
      lines.push("data:".to_owned());
      if json_lines {
        for item in &self.data {
          lines.push(format!("{}", json!(item)));
        }
      } else {
        lines.push(format!("{}", json!(self.data)));
      }
    }
    lines
  }

  /// Extract the vector of rows as Index Maps of JSON values
  /// Good for post-processing results
  pub fn to_vec(&self) -> Vec<IndexMap<String, Value>> {
    self.data.clone()
  }
  
  /// JSON object of row arrays only
  pub fn json_rows(&self) -> Value {
    json!(self.data)
  }

  /// final output as vector of JSON-serializable array
  pub fn rows(&self) -> Vec<String> {
    let mut lines = Vec::with_capacity(self.data.len());
    for row in &self.data {
      lines.push(json!(row).to_string());
    }
    lines
  }

}


#[derive(Debug, Clone, Serialize)]
pub enum DataSet {
   WithRows(usize, Vec<IndexMap<String, Value>>),
   Count(usize) 
}

impl DataSet {
  pub fn from_count_and_rows(count: usize, rows: Vec<IndexMap<String, Value>>, opts: &OptionSet) -> Self {
    match opts.read_mode() {
      ReadMode::Sync | ReadMode::PreviewAsync => DataSet::WithRows(count, rows),
      ReadMode::Async => DataSet::Count(count),
    }
  }
}


pub fn to_index_map(row: &[serde_json::Value], headers: &[String]) -> IndexMap<String, Value> {
    let mut hm: IndexMap<String, serde_json::Value> = IndexMap::new();
    let mut sub_index = 0;
    for hk in headers {
        if let Some(cell) = row.get(sub_index) {
            hm.insert(hk.to_owned(), cell.to_owned());
        } 
        sub_index += 1;
    }
    hm
}

pub fn match_sheet_name_and_index(workbook: &mut Sheets<BufReader<File>>, opts: &OptionSet) -> (Option<Arc<String>>, Vec<String>, usize) {
  let mut sheet_index = 0;
  let sheet_names = workbook.worksheets().into_iter().map(|ws| ws.0).collect::<Vec<String>>();
  if let Some(sheet_key) = opts.sheet.clone() {
      let key_string = sheet_key.strip_spaces().to_lowercase();
      if let Some(s_index) = sheet_names.clone().into_iter().position(|sn| sn.strip_spaces().to_lowercase() == key_string) {
          sheet_index = s_index;
      }
  }
  if let Some(s_name) = sheet_names.get(sheet_index).map(|s| s.to_owned()) {
    (Some(Arc::new(s_name)), sheet_names, sheet_index)
  } else {
    (None, sheet_names, sheet_index)
  }
}

