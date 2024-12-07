use indexmap::IndexMap;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct DataSet {
    pub sheet_index: usize,
    pub sheet_key: String,
    pub sheet_refs: Vec<String>,
    pub headers: Vec<String>,
    pub data: Vec<IndexMap<String, serde_json::Value>>
}

impl DataSet {
    pub fn new(headers: &[String], data: &[IndexMap<String, serde_json::Value>], sheet_key: &str, sheet_index: usize, sheet_refs: &[String]) -> Self {
        DataSet {
            sheet_index: sheet_index,
            sheet_key: sheet_key.to_owned(),
            sheet_refs: sheet_refs.to_vec(),
            headers: headers.to_vec(),
            data: data.to_vec()
        }
    }

    pub fn to_json(&self) -> String {
        json!({
            "sheet_index": self.sheet_index,
            "sheet_key": self.sheet_key,
            "sheet_refs": self.sheet_refs,
            "headers": self.headers,
            "data": self.data
        }).to_string()
    }
}

pub fn to_dictionary(row: &[serde_json::Value], headers: &[String]) -> IndexMap<String, serde_json::Value> {
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