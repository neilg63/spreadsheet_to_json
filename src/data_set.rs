use std::{fs::File, io::BufReader};
use calamine::{Reader, Sheets};
use heck::ToSnakeCase;
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::{json, Value};

use crate::{OptionSet, PathData, ReadMode};


/// Core info about a spreadsheet with extension, matched worksheet name and index an all worksheet keys
#[derive(Debug, Clone)]
pub struct WorkbookInfo {
    pub filename: String,
    pub extension: String,
    pub selected: Option<Vec<String>>,
    pub sheets: Vec<String>,
}

impl WorkbookInfo {
    pub fn new(path_data: &PathData, selected: &[String], sheet_refs: &[String]) -> Self {
        WorkbookInfo {
            extension: path_data.extension(),
            filename: path_data.filename(), 
            selected: Some(selected.to_vec()),
            sheets: sheet_refs.to_vec(),
        }
    }

    pub fn simple(path_data: &PathData) -> Self {
        let sheet_name = "single";
        WorkbookInfo {
            extension: path_data.extension(),
            filename: path_data.filename(), 
            selected: None,
            sheets: vec![sheet_name.to_owned()],
        }
    }

    pub fn ext(&self) -> String {
        self.extension.to_owned()
    }

    pub fn name(&self) -> String {
        self.filename.to_owned()
    }

    pub fn sheet(&self, index: usize) -> (String, usize) {
      let sheet_name = self.sheets.get(index).unwrap_or(&"single".to_owned()).to_owned();
      (sheet_name, index)
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
    pub selected: Option<Vec<String>>,
    pub sheets: Vec<String>,
    pub keys: Vec<String>,
    pub num_rows: usize,
    pub data: SpreadData,
    pub out_ref: Option<String>
}

impl ResultSet {

  /// Instantiate with Core workbook info, header keys, data set and optional output reference
  pub fn new(info: &WorkbookInfo, keys: &[String], data_set: DataSet, out_ref: Option<&str>) -> Self {
    let (num_rows, data) = match data_set {
      DataSet::WithRows(size, rows) => (size, rows),
      DataSet::Count(size) => (size, vec![])
    };
    ResultSet {
      extension: info.ext(),
      filename: info.name(), 
      selected: info.selected.clone(),
      sheets: info.sheets(),
      keys: keys.to_vec(),
      num_rows,
      data: SpreadData::from_single(data),
      out_ref: out_ref.map(|s| s.to_string())
    }
  }

  pub fn from_multiple(sheet_sets: &[(String, Vec<IndexMap<String, Value>>, usize)], info: &WorkbookInfo) -> Self {
    let mut keys = vec![];
    let mut num_rows = 0;
    let mut data = IndexMap::new();
    let selected = None;
    let mut sheets = vec![];
    let filename = info.filename.clone();
    let extension = info.extension.clone();
    let mut sheet_index: usize = 0;
    for (sheet_ref, sheet_data, total) in sheet_sets {
      data.insert(sheet_ref.to_owned(), sheet_data.to_owned());
      num_rows += total;
      sheets.push(sheet_ref.clone());
      if sheet_index == 0 && sheet_data.len() > 0 {
        keys = sheet_data[0].keys().map(|k| k.to_owned()).collect();
      }
      sheet_index += 1;
    }
    ResultSet {
      extension,
      filename, 
      selected,
      sheets,
      keys,
      num_rows,
      data: SpreadData::Multiple(data),
      out_ref: None
    }
  }


  /// Full result set as JSON with criteria, options and data in synchronous mode
  pub fn to_json(&self) -> Value {
    let mut result = json!({
      "name": self.filename,
      "extension": self.extension,
      "selected": self.selected.clone().unwrap_or(vec![]),
      "sheets": self.sheets,
      "num_rows": self.num_rows,
      "fields": self.keys,
      "data": self.data.to_json(),
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
      format!("sheet: name: {}", self.selected.clone().unwrap_or(vec![]).join(", ")),
      format!("sheets: {}", self.sheets.join(", ")),
      format!("row count: {}", self.num_rows),
      format!("fields: {}", self.keys.join(",")),
    ];
    if let Some(out_ref_str) = self.out_ref.clone() {
      lines.push(format!("output reference: {}", out_ref_str));
    } else {
      lines.push("data:".to_owned());
      if json_lines {
        let has_many_sheets = self.sheets.len() > 1;
        for (sheet_ref, items  ) in &self.data.all_sheets() {
          if has_many_sheets {
            lines.push(format!("Sheet: {} :", sheet_ref));
          }
          for item in items {
            lines.push(format!("{}", json!(item)));
          }
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
    self.data.first_sheet().clone()
  }
  
  /// JSON object of row arrays only
  pub fn json_rows(&self) -> Value {
    json!(self.data)
  }

  /// final output as vector of JSON-serializable array
  pub fn rows(&self) -> Vec<String> {
    let sheet = self.data.first_sheet();
    let mut lines = Vec::with_capacity(sheet.len());
    for row in &self.data.first_sheet() {
      lines.push(json!(row).to_string());
    }
    lines
  }

}

#[derive(Debug, Clone, Serialize)]
pub enum SpreadData {
   Single(Vec<IndexMap<String, Value>>),
   Multiple(IndexMap<String, Vec<IndexMap<String, Value>>>)
}

impl SpreadData {
  pub fn from_single(rows: Vec<IndexMap<String, Value>>) -> Self {
    SpreadData::Single(rows)
  }

  pub fn from_multiple(rows: IndexMap<String, Vec<IndexMap<String, Value>>>) -> Self {
    SpreadData::Multiple(rows)
  }

  pub fn first_sheet(&self) -> Vec<IndexMap<String, Value>> {
    match self {
      SpreadData::Single(rows) => rows.to_owned(),
      SpreadData::Multiple(sheet_map) => sheet_map.values().next().unwrap().to_owned()
    }
  }

  pub fn all_sheets(&self) -> IndexMap<String, Vec<IndexMap<String, Value>>> {
    match self {
      SpreadData::Single(rows) => {
        let mut hm = IndexMap::new();
        hm.insert("single".to_owned(), rows.to_owned());
        hm  
      },
      SpreadData::Multiple(sheet_map) => sheet_map.to_owned()
    }
  }

  pub fn to_json(&self) -> Value {
    match self {
      SpreadData::Single(sheet) => json!(sheet),
      SpreadData::Multiple(sheet_map) => json!(sheet_map)
    }
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
      ReadMode::Sync | ReadMode::PreviewMultiple => DataSet::WithRows(count, rows),
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

pub fn match_sheet_name_and_index(workbook: &mut Sheets<BufReader<File>>, opts: &OptionSet) -> (Vec<String>, Vec<String>, Vec<usize>) {
  let mut sheet_indices = vec![];
  let mut selected_names: Vec<String> = vec![];
  let sheet_names = workbook.worksheets().into_iter().map(|ws| ws.0).collect::<Vec<String>>();
  if let Some(sheet_keys) = opts.selected.clone() {
      for sheet_key in sheet_keys {
          if let Some(sheet_index) = sheet_names.iter().position(|s| s.to_snake_case() == sheet_key.to_snake_case()) {
              sheet_indices.push(sheet_index);
              selected_names.push(sheet_names[sheet_index].clone());
          }
      }
  }
  if sheet_indices.len() < 1 {
    sheet_indices = vec![0];
    if sheet_names.len() > 0 {
      selected_names.push(sheet_names[0].clone());
    }
  }
  (selected_names, sheet_names, sheet_indices)
}

