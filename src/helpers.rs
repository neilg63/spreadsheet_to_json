use calamine::Data;
use indexmap::IndexMap;
use serde_json::{Number, Value};


pub fn json_object_to_indexmap(json: Value) -> Option<IndexMap<String, Value>> {
  json.as_object().map(|obj| {
    obj.iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect::<IndexMap<String, Value>>()
  })
}

pub fn json_object_to_calamine_data(json: Value) -> Vec<Data> {
  let mut cells = vec![];
  if let Some(obj) = json.as_object() {
    for (_k, v) in obj.to_owned() {
      let cell = match v {
        Value::Number(fl) => Data::Float(fl.as_f64().unwrap_or(0.0)),
        Value::Bool(b) => Data::Bool(b),
        Value::String(s) => Data::String(s),
        Value::Null => Data::Empty,
        _ => Data::Empty,
      };
      cells.push(cell);
    }
  }
  cells      
}

pub fn json_array_to_indexmaps(json: Value) -> Vec<IndexMap<String, Value>> {
    json.as_array().unwrap().iter()
    .map(|v| v.to_owned()).filter_map(json_object_to_indexmap)
    .collect()
}

pub fn float_value(value: f64) -> Value {
  Value::Number(Number::from_f64(value).unwrap_or(Number::from_f64(0.0).unwrap()))
}

pub fn integer_value(value: i64) -> Value {
  Value::Number(Number::from_i128(value as i128).unwrap_or(Number::from_i128(0).unwrap()))
}

pub fn string_value(value: &str) -> Value {
  Value::String(String::from(value))
}

pub fn bool_value(value: bool) -> Value {
  Value::Bool(value)
}