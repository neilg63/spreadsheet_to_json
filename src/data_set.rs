use indexmap::IndexMap;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct DataSet {
    pub filename: String,
    pub extension: String,
    pub sheet: (String, usize),
    pub sheets: Vec<String>,
    pub keys: Vec<String>,
    pub data: Vec<IndexMap<String, Value>>
}

impl DataSet {
    pub fn new(name: &str, extension: &str, keys: &[String], data: &[IndexMap<String, Value>], sheet: &str, sheet_index: usize, sheet_refs: &[String]) -> Self {
        DataSet {
            extension: extension.to_owned(),
            filename: name.to_owned(), 
            sheet: (sheet.to_owned(), sheet_index),
            sheets: sheet_refs.to_vec(),
            keys: keys.to_vec(),
            data: data.to_vec()
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
            "data": self.data
        })
    }
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