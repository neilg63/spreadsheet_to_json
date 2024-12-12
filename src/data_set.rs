use indexmap::IndexMap;
use serde::Serialize;
use serde_json::{json, Value};



#[derive(Debug, Clone)]
pub struct WorkbookInfo {
    pub filename: String,
    pub extension: String,
    pub sheet: (String, usize),
    pub sheets: Vec<String>,
}

impl WorkbookInfo {
    pub fn new(name: &str, extension: &str, sheet: &str, sheet_index: usize, sheet_refs: &[String]) -> Self {
        WorkbookInfo {
            extension: extension.to_owned(),
            filename: name.to_owned(), 
            sheet: (sheet.to_owned(), sheet_index),
            sheets: sheet_refs.to_vec(),
        }
    }

    pub fn simple(name: &str, extension: &str) -> Self {
        let sheet_name = "single";
        WorkbookInfo {
            extension: extension.to_owned(),
            filename: name.to_owned(), 
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

#[derive(Debug, Clone)]
pub struct ResultSet {
    pub filename: String,
    pub extension: String,
    pub sheet: (String, usize),
    pub sheets: Vec<String>,
    pub keys: Vec<String>,
    pub num_rows: usize,
    pub data: Vec<IndexMap<String, Value>>,
}

impl ResultSet {
    pub fn new(info: WorkbookInfo, keys: &[String], data_set: DataSet) -> Self {
        let (num_rows, data) = match data_set {
            DataSet::Rows(rows) => (rows.len(), rows),
            DataSet::Preview(size, rows, iterator) => (size, rows, iterator),
            DataSet::Count(size, iterator) => (size, vec![], iterator)
        };
        ResultSet {
            extension: info.ext(),
            filename: info.name(), 
            sheet: info.sheet(),
            sheets: info.sheets(),
            keys: keys.to_vec(),
            num_rows,
            data
        }
    }

    pub fn to_json(&self) -> Value {
        json!({
            "name": self.filename,
            "extension": self.extension,
            "sheet": {
                "key": self.sheet.0,
                "index": self.sheet.1
            },
            "sheets": self.sheets,
            "fields": self.keys,
            "num_rows": self.num_rows,
            "data": self.data
        })
    }
}


#[derive(Debug, Clone, Serialize)]
pub enum DataSet<T: Iterator> {
   Rows(Vec<IndexMap<String, Value>>),
   Preview(usize, Vec<IndexMap<String, Value>>, Option<T>),
   Count(usize, Option<T>) 
}


pub fn to_dictionary(row: &[serde_json::Value], headers: &[String]) -> IndexMap<String, Value> {
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