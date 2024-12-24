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

  pub fn from_multiple(sheets: &[SheetDataSet], info: &WorkbookInfo) -> Self {
    
    
    let selected = None;
    let mut sheet_names = vec![];
    let filename = info.filename.clone();
    let extension = info.extension.clone();
    let mut keys: Vec<String> = vec![];
    let mut num_rows = 0;
    let mut sheet_index: usize = 0;
    for sheet in sheets {
      num_rows += sheet.num_rows;
      sheet_names.push(sheet.name());
      if sheet_index == 0 {
        keys = sheet.keys.clone();
      }
      sheet_index += 1;
    }
    ResultSet {
      extension,
      filename, 
      selected,
      sheets: sheet_names,
      keys,
      num_rows,
      data: SpreadData::Multiple(sheets.to_vec()),
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
        for sheet in &self.data.sheets() {
          if has_many_sheets {
            lines.push(format!("Sheet: {} :", sheet.name()));
          }
          for item in &sheet.rows {
            lines.push(format!("{}", json!(item)));
          }
        }
      } else {
        lines.push(format!("{}", self.data.to_json()));
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
pub struct SheetDataSet {
  pub sheet: (String, String),
  pub num_rows: usize,
  pub keys: Vec<String>,
  pub rows: Vec<IndexMap<String, Value>>
}

impl SheetDataSet {

  

  pub fn new(name: &str, keys: &[String], rows: &[IndexMap<String, Value>], total: usize) -> Self {
    Self {
      sheet: (name.to_string(), name.to_snake_case()),
      keys: keys.to_vec(),
      rows: rows.to_vec(),
      num_rows: total
    }
  }

  pub fn key(&self) -> String {
    self.sheet.1.clone()
  }

  pub fn name(&self) -> String {
    self.sheet.0.clone()
  }
}

#[derive(Debug, Clone, Serialize)]
pub enum SpreadData {
   Single(Vec<IndexMap<String, Value>>),
   Multiple(Vec<SheetDataSet>)
}

impl SpreadData {
  pub fn from_single(rows: Vec<IndexMap<String, Value>>) -> Self {
    SpreadData::Single(rows)
  }

  pub fn from_multiple(sheet_data: &[SheetDataSet]) -> Self {
    SpreadData::Multiple(sheet_data.to_owned())
  }

  pub fn first_sheet(&self) -> Vec<IndexMap<String, Value>> {
    match self {
      SpreadData::Single(rows) => rows.to_owned(),
      SpreadData::Multiple(sheets) => {
        if let Some(sheet) = sheets.get(0) {
          sheet.rows.to_owned()
        } else {
          vec![]
        }
      }
    }
  }

  // Only for preview multiple mode
  pub fn sheets(&self) -> Vec<SheetDataSet> {
    match self {
      SpreadData::Single(_) => vec![],
      SpreadData::Multiple(sheets) => sheets.to_owned()
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
  if sheet_indices.len() < 1 && opts.indices.len() > 0 {
    for s_index in opts.indices.clone() {
      let sheet_index = s_index as usize;
      if let Some(sheet_name) = sheet_names.get(sheet_index) {
          sheet_indices.push(sheet_index);
          selected_names.push(sheet_name.to_owned());
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

