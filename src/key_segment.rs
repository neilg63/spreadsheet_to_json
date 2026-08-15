use serde_json::{Map, Value};
use std::sync::Arc;

/// A sortable identifier used to discriminate between array items -- either a plain
/// string label ("west") or an integer ("2015"). Kept as a small union type (rather than
/// always coercing to a string) so identifiers extracted from header labels render as
/// the JSON type they actually are (`2015`, not `"2015"`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Identifier {
    String(Arc<str>),
    Integer(i64),
}

impl Identifier {
    pub fn from_string(id: &str) -> Self {
        Identifier::String(Arc::from(id))
    }

    /// Parses `id` as an integer identifier. Only ever fed numeric segments already
    /// extracted from header labels (e.g. "2015" out of "longevity_2015_female"), where
    /// a parse failure would mean something has gone wrong upstream in the extraction
    /// itself, not in this value -- confirmed as an acceptable fallback for that
    /// constrained calling context, unlike the general-purpose sanitize-don't-guess
    /// rule this crate otherwise follows for arbitrary user input.
    pub fn from_int_str(id: &str) -> Self {
        Identifier::Integer(id.parse::<i64>().unwrap_or_default())
    }

    pub fn from_int(id: i64) -> Self {
        Identifier::Integer(id)
    }

    /// The serde_json::Value this identifier renders as when written into a field.
    pub fn to_value(&self) -> Value {
        match self {
            Identifier::String(s) => Value::String(s.to_string()),
            Identifier::Integer(n) => Value::Number((*n).into()),
        }
    }

    /// Parses an `Identifier` from JSON -- a plain JSON string becomes `String`, a plain
    /// JSON number becomes `Integer`, matching `to_value`'s own output shape so a
    /// round-trip through JSON is lossless for both variants.
    pub fn from_json(json: &Value) -> Option<Self> {
        match json {
            Value::String(s) => Some(Identifier::String(Arc::from(s.as_str()))),
            Value::Number(n) => n.as_i64().map(Identifier::Integer),
            _ => None,
        }
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Identifier::String(s) => write!(f, "{}", s),
            Identifier::Integer(n) => write!(f, "{}", n),
        }
    }
}

/// Describes where a column's cell value lands in the (possibly nested) output row.
/// Recursive variants wrap their continuation in `Arc` (not `Box`) to keep the type's
/// size finite -- same reasoning as `Format::Array`'s `Arc<Format>`: `Column` (and
/// therefore its `key: Option<KeySegment>`) is cloned repeatedly by `resolve_columns()`
/// during column resolution, so an O(1) refcount bump matters here the same way it does
/// for `Format`. Nothing in this tree needs `Box`'s unique-ownership guarantee.
#[derive(Debug, Clone, PartialEq)]
pub enum KeySegment {
    /// Column is fully omitted from output.
    Excluded,
    /// Flat leaf -- today's only behavior, insert directly under this key.
    Simple(Arc<str>),
    /// Descend into (creating if needed) a plain nested object under this key, then
    /// continue the rest of the path inside it.
    Object(Arc<str>, Arc<KeySegment>),
    /// Find-or-create an item in the named array whose `key_field` equals `identifier`,
    /// then continue the rest of the path *inside* that item. Two columns land in the
    /// same item only when their *entire* chain of Array/InnerObject identifiers agree,
    /// not just this one segment -- see `matching_signature` below.
    Array(Arc<str>, Identifier, Arc<str>, Arc<KeySegment>),
    /// Inline a literal-valued field on the *current* item (no new nesting level), then
    /// continue the rest of the path in the same item. Used to flatten multiple
    /// discriminators onto one array item instead of nesting each one.
    InnerObject(Identifier, Arc<str>, Arc<KeySegment>),
    /// Push the column's own resolved value directly into the named array, as a bare
    /// scalar -- no wrapping object, no discriminator, always appended. Distinct from
    /// `Array`, whose items are always objects (at least the `key_field` is set) --
    /// there's no way to get a plain array of raw values through `Array`/`InnerObject`
    /// alone. Order is whatever order the matched columns were processed in (column
    /// position in the sheet), the same choice already made for the analogous
    /// numbered-fields-to-array case at the spread-cli layer.
    PlainArray(Arc<str>),
}

impl KeySegment {
    /// Parses a `KeySegment` from JSON -- the primary way any client crate (a frontend
    /// UI building a JSON payload, a Web API, etc.) reaches the full tree without writing
    /// Rust or touching calamine/csv directly. A plain JSON string is shorthand for
    /// `Simple` (matches `Column.key`'s existing plain-string convention); anything else
    /// needs a tagged object with a `"type"` field selecting the variant:
    ///
    /// - `"excluded"` -- no other fields
    /// - `"simple"` -- `"key"` (string)
    /// - `"object"` -- `"key"` (string), `"next"` (nested KeySegment)
    /// - `"array"` -- `"container"` (string), `"identifier"` (string or number),
    ///   `"key_field"` (string), `"next"` (nested KeySegment)
    /// - `"inner_object"` -- `"identifier"` (string or number), `"field"` (string),
    ///   `"next"` (nested KeySegment)
    /// - `"plain_array"` -- `"container"` (string)
    ///
    /// Returns `None` on anything malformed (unknown type, missing/wrong-typed field) --
    /// same sanitize-don't-guess stance as the rest of this crate's parsing.
    pub fn from_json(json: &Value) -> Option<Self> {
        match json {
            Value::String(s) => Some(KeySegment::Simple(Arc::from(s.as_str()))),
            Value::Object(map) => {
                let get_str = |field: &str| map.get(field).and_then(|v| v.as_str());
                let get_next = || map.get("next").and_then(KeySegment::from_json).map(Arc::new);
                let get_id = || map.get("identifier").and_then(Identifier::from_json);
                match map.get("type").and_then(|v| v.as_str())? {
                    "excluded" => Some(KeySegment::Excluded),
                    "simple" => get_str("key").map(|s| KeySegment::Simple(Arc::from(s))),
                    "object" => Some(KeySegment::Object(Arc::from(get_str("key")?), get_next()?)),
                    "array" => Some(KeySegment::Array(
                        Arc::from(get_str("container")?),
                        get_id()?,
                        Arc::from(get_str("key_field")?),
                        get_next()?,
                    )),
                    "inner_object" => Some(KeySegment::InnerObject(get_id()?, Arc::from(get_str("field")?), get_next()?)),
                    "plain_array" => Some(KeySegment::PlainArray(Arc::from(get_str("container")?))),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for KeySegment {
    /// A flat, single-string fallback for contexts that only ever show one name per
    /// column (header/metadata listings) -- not a serialization of the whole tree. Shows
    /// the outermost field name at this segment; nested detail is only ever realized by
    /// `insert_key_segment` when actually building a row.
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            KeySegment::Excluded => write!(f, ""),
            KeySegment::Simple(key) => write!(f, "{}", key),
            KeySegment::Object(key, _) => write!(f, "{}", key),
            KeySegment::Array(container, ..) => write!(f, "{}", container),
            KeySegment::InnerObject(_, field, _) => write!(f, "{}", field),
            KeySegment::PlainArray(container) => write!(f, "{}", container),
        }
    }
}

/// Walks `segment`, inserting `value` at the location it describes within `current`.
/// `current` is a plain JSON object -- the top-level row map for a column with no
/// nesting, or a nested/array-item object for anything reached via `Object`/`Array`.
pub fn insert_key_segment(current: &mut Map<String, Value>, segment: &KeySegment, value: Value) {
    match segment {
        KeySegment::Excluded => {}
        KeySegment::Simple(key) => {
            current.insert(key.to_string(), value);
        }
        KeySegment::Object(key, next) => {
            let nested = current
                .entry(key.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(nested_map) = nested {
                insert_key_segment(nested_map, next, value);
            }
        }
        KeySegment::Array(container, id, key_field, next) => {
            let signature = matching_signature(key_field, id, next);
            let arr = current
                .entry(container.to_string())
                .or_insert_with(|| Value::Array(vec![]));
            let Value::Array(items) = arr else { return };
            let existing_index = items.iter().position(|item| signature_matches(item, &signature));
            let item_index = match existing_index {
                Some(idx) => idx,
                None => {
                    items.push(Value::Object(Map::new()));
                    items.len() - 1
                }
            };
            let Value::Object(item_map) = &mut items[item_index] else {
                return;
            };
            // Idempotent whether the item was just created or reused -- always leaves
            // this segment's own discriminator field set.
            item_map.insert(key_field.to_string(), id.to_value());
            insert_key_segment(item_map, next, value);
        }
        KeySegment::InnerObject(id, field, next) => {
            current.insert(field.to_string(), id.to_value());
            insert_key_segment(current, next, value);
        }
        KeySegment::PlainArray(container) => {
            // The array itself always exists once any column maps to it, even if every
            // matched cell in this row turns out blank -- ["title": "Title A", "downloads":
            // []], not "downloads" missing entirely. Create the entry unconditionally
            // *before* deciding whether to push, not after: an early return here would
            // skip creating it at all when this happens to be the only (blank) column
            // seen so far for this row.
            let arr = current
                .entry(container.to_string())
                .or_insert_with(|| Value::Array(vec![]));
            // A null (or, since a blank CSV/text cell comes through as "" rather than a
            // genuine null -- there's no such thing as a null CSV field -- an empty
            // string too) means "this sequential slot didn't apply" (e.g. download_2 was
            // blank while download_1/download_3 had real files), not "a real element
            // whose value happens to be empty" -- dropped rather than kept as a
            // positional entry, so ["file-1.pdf", "", "file-3.pdf"] (or the null
            // equivalent from a native xlsx/ods empty cell) becomes just
            // ["file-1.pdf", "file-3.pdf"].
            let is_blank = value.is_null() || matches!(&value, Value::String(s) if s.is_empty());
            if !is_blank {
                if let Value::Array(items) = arr {
                    items.push(value);
                }
            }
        }
    }
}

/// Every (field, identifier) pair that determines whether two columns land in the same
/// array item -- this Array's own, plus any InnerObjects chained directly after it,
/// stopping at the first segment that isn't an InnerObject. An InnerObject sets a
/// sibling field on the very same item rather than starting a new nesting level, so it
/// has to agree too before two columns are considered "the same item"; anything past an
/// Object/another Array/a terminal Simple is a genuinely separate substructure and
/// doesn't need to factor into this Array's own matching decision.
fn matching_signature(key_field: &Arc<str>, id: &Identifier, next: &KeySegment) -> Vec<(Arc<str>, Identifier)> {
    let mut signature = vec![(key_field.clone(), id.clone())];
    let mut cursor = next;
    while let KeySegment::InnerObject(inner_id, inner_field, inner_next) = cursor {
        signature.push((inner_field.clone(), inner_id.clone()));
        cursor = inner_next;
    }
    signature
}

fn signature_matches(item: &Value, signature: &[(Arc<str>, Identifier)]) -> bool {
    let Value::Object(map) = item else { return false };
    signature
        .iter()
        .all(|(field, id)| map.get(field.as_ref()) == Some(&id.to_value()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(segments_and_values: &[(KeySegment, Value)]) -> Value {
        let mut map = Map::new();
        for (segment, value) in segments_and_values {
            insert_key_segment(&mut map, segment, value.clone());
        }
        Value::Object(map)
    }

    #[test]
    fn test_identifier_from_json_reads_strings_and_numbers() {
        assert_eq!(Identifier::from_json(&serde_json::json!("north")), Some(Identifier::String(Arc::from("north"))));
        assert_eq!(Identifier::from_json(&serde_json::json!(2015)), Some(Identifier::Integer(2015)));
        assert_eq!(Identifier::from_json(&serde_json::json!(true)), None);
        assert_eq!(Identifier::from_json(&serde_json::json!(null)), None);
    }

    #[test]
    fn test_key_segment_from_json_plain_string_is_simple_shorthand() {
        assert!(matches!(KeySegment::from_json(&serde_json::json!("weight")), Some(KeySegment::Simple(k)) if &*k == "weight"));
    }

    #[test]
    fn test_key_segment_from_json_excluded() {
        assert!(matches!(
            KeySegment::from_json(&serde_json::json!({"type": "excluded"})),
            Some(KeySegment::Excluded)
        ));
    }

    #[test]
    fn test_key_segment_from_json_object_nests_recursively() {
        let json = serde_json::json!({
            "type": "object",
            "key": "sales",
            "next": {"type": "object", "key": "north", "next": "value"}
        });
        let segment = KeySegment::from_json(&json).expect("should parse");
        // exercise it through the real row-insertion path, not just check the shape
        let result = run(&[(segment, Value::from(4500000))]);
        assert_eq!(result, serde_json::json!({"sales": {"north": {"value": 4500000}}}));
    }

    #[test]
    fn test_key_segment_from_json_plain_array() {
        let json = serde_json::json!({"type": "plain_array", "container": "files"});
        let segment = KeySegment::from_json(&json).expect("should parse");
        let result = run(&[
            (segment.clone(), Value::String("a.pdf".into())),
            (segment, Value::String("b.pdf".into())),
        ]);
        assert_eq!(result, serde_json::json!({"files": ["a.pdf", "b.pdf"]}));
    }

    #[test]
    fn test_key_segment_from_json_array_with_inner_object_round_trips_the_longevity_example() {
        // The full tree from the year/gender/class example, built entirely from JSON --
        // this is the shape a frontend UI would send, not something hand-typed via --keys.
        fn column_json(year: i64, gender: &str, class: &str) -> Value {
            serde_json::json!({
                "type": "array",
                "container": "longevity",
                "identifier": year,
                "key_field": "year",
                "next": {
                    "type": "inner_object",
                    "identifier": gender,
                    "field": "gender",
                    "next": {
                        "type": "inner_object",
                        "identifier": class,
                        "field": "class",
                        "next": "value"
                    }
                }
            })
        }
        let segments = [
            (KeySegment::from_json(&column_json(2015, "female", "upperclass")).unwrap(), Value::from(87.9)),
            (KeySegment::from_json(&column_json(2015, "male", "upperclass")).unwrap(), Value::from(83.8)),
        ];
        let result = run(&segments);
        assert_eq!(
            result,
            serde_json::json!({"longevity": [
                {"year": 2015, "gender": "female", "class": "upperclass", "value": 87.9},
                {"year": 2015, "gender": "male", "class": "upperclass", "value": 83.8}
            ]})
        );
    }

    #[test]
    fn test_key_segment_from_json_rejects_unknown_type_and_missing_fields() {
        assert_eq!(KeySegment::from_json(&serde_json::json!({"type": "bogus"})), None);
        // "object" requires both "key" and "next"
        assert_eq!(KeySegment::from_json(&serde_json::json!({"type": "object", "key": "sales"})), None);
        assert_eq!(KeySegment::from_json(&serde_json::json!({"type": "object", "next": "value"})), None);
        // no "type" at all on an object that isn't the plain-string shorthand
        assert_eq!(KeySegment::from_json(&serde_json::json!({"key": "sales"})), None);
    }

    #[test]
    fn test_simple_matches_todays_flat_behavior() {
        let result = run(&[(KeySegment::Simple(Arc::from("country_code")), Value::String("AFG".into()))]);
        assert_eq!(result, serde_json::json!({"country_code": "AFG"}));
    }

    #[test]
    fn test_excluded_inserts_nothing() {
        let result = run(&[(KeySegment::Excluded, Value::String("skip me".into()))]);
        assert_eq!(result, serde_json::json!({}));
    }

    #[test]
    fn test_object_nests_and_merges_sibling_columns() {
        // sales_west, sales_east both descend into the same "sales" object
        let segments = [
            (
                KeySegment::Object(Arc::from("sales"), Arc::new(KeySegment::Simple(Arc::from("west")))),
                Value::Number(19812.into()),
            ),
            (
                KeySegment::Object(Arc::from("sales"), Arc::new(KeySegment::Simple(Arc::from("east")))),
                Value::Number(17293.into()),
            ),
        ];
        let result = run(&segments);
        assert_eq!(result, serde_json::json!({"sales": {"west": 19812, "east": 17293}}));
    }

    #[test]
    fn test_array_pushes_positionally_with_no_discriminator() {
        // file_1, file_2, file_3 -- Array with an Identifier that never matches an
        // existing item's field (a fresh Identifier per column) always appends.
        let segments = [
            (
                KeySegment::Array(Arc::from("files"), Identifier::from_int(0), Arc::from("_idx"), Arc::new(KeySegment::Simple(Arc::from("value")))),
                Value::String("sales-data.xlsx".into()),
            ),
            (
                KeySegment::Array(Arc::from("files"), Identifier::from_int(1), Arc::from("_idx"), Arc::new(KeySegment::Simple(Arc::from("value")))),
                Value::String("marketing-report.pdf".into()),
            ),
        ];
        let result = run(&segments);
        assert_eq!(
            result,
            serde_json::json!({"files": [
                {"_idx": 0, "value": "sales-data.xlsx"},
                {"_idx": 1, "value": "marketing-report.pdf"}
            ]})
        );
    }

    #[test]
    fn test_plain_array_pushes_bare_scalars_with_no_object_wrapper() {
        // file_1, file_2, file_3 -- unlike KeySegment::Array (which always wraps items in
        // an object with at least a key_field set), PlainArray produces a genuinely flat
        // array of raw values, no discriminator needed at all.
        let segments = [
            (KeySegment::PlainArray(Arc::from("files")), Value::String("file_1.pdf".into())),
            (KeySegment::PlainArray(Arc::from("files")), Value::String("file_2.pdf".into())),
            (KeySegment::PlainArray(Arc::from("files")), Value::String("file_3.pdf".into())),
        ];
        let result = run(&segments);
        assert_eq!(result, serde_json::json!({"files": ["file_1.pdf", "file_2.pdf", "file_3.pdf"]}));
    }

    #[test]
    fn test_plain_array_drops_null_elements_rather_than_keeping_them_positional() {
        // download_1, download_2 (blank), download_3 -- the blank slot is dropped
        // entirely, not kept as a positional null.
        let segments = [
            (KeySegment::PlainArray(Arc::from("downloads")), Value::String("file-1.pdf".into())),
            (KeySegment::PlainArray(Arc::from("downloads")), Value::Null),
            (KeySegment::PlainArray(Arc::from("downloads")), Value::String("file-3.pdf".into())),
        ];
        let result = run(&segments);
        assert_eq!(result, serde_json::json!({"downloads": ["file-1.pdf", "file-3.pdf"]}));
    }

    #[test]
    fn test_plain_array_also_drops_empty_strings_since_csv_blanks_are_never_actually_null() {
        // A blank CSV/text-cell field comes through as Value::String(""), never a
        // genuine null -- CSV has no native null -- so the null check alone wouldn't
        // catch the practical case this feature exists for.
        let segments = [
            (KeySegment::PlainArray(Arc::from("downloads")), Value::String("file-1.pdf".into())),
            (KeySegment::PlainArray(Arc::from("downloads")), Value::String("".into())),
            (KeySegment::PlainArray(Arc::from("downloads")), Value::String("file-3.pdf".into())),
        ];
        let result = run(&segments);
        assert_eq!(result, serde_json::json!({"downloads": ["file-1.pdf", "file-3.pdf"]}));
    }

    #[test]
    fn test_plain_array_stays_an_empty_array_not_absent_when_every_matched_cell_is_blank() {
        // Regression: an early return on the blank check used to skip creating the
        // array entry at all, so a row where every download_N column was blank had no
        // "downloads" key whatsoever, rather than "downloads": [].
        let segments = [
            (KeySegment::PlainArray(Arc::from("downloads")), Value::String("".into())),
            (KeySegment::PlainArray(Arc::from("downloads")), Value::Null),
        ];
        let result = run(&segments);
        assert_eq!(result, serde_json::json!({"downloads": []}));
    }

    #[test]
    fn test_array_inner_object_merges_matching_year_into_one_item() {
        // longevity_2015_female_upperclass and longevity_2015_male_upperclass: same
        // year -> same item; different gender -> different nested branch.
        fn seg(gender: &str, class: &str) -> KeySegment {
            KeySegment::Array(
                Arc::from("longevity"),
                Identifier::from_int_str("2015"),
                Arc::from("year"),
                Arc::new(KeySegment::Object(
                    Arc::from(gender),
                    Arc::new(KeySegment::Simple(Arc::from(class))),
                )),
            )
        }
        let segments = [
            (seg("female", "upperclass"), Value::from(87.9)),
            (seg("male", "upperclass"), Value::from(83.8)),
            (seg("female", "lowerclass"), Value::from(80.4)),
            (seg("male", "lowerclass"), Value::from(76.9)),
        ];
        let result = run(&segments);
        assert_eq!(
            result,
            serde_json::json!({"longevity": [
                {
                    "year": 2015,
                    "female": {"upperclass": 87.9, "lowerclass": 80.4},
                    "male": {"upperclass": 83.8, "lowerclass": 76.9}
                }
            ]})
        );
    }

    #[test]
    fn test_array_with_chained_inner_objects_produces_flat_tidy_items() {
        // The full-chain-matching case that motivated matching_signature: four columns
        // all sharing year=2015 must NOT collapse onto one item, because their gender/
        // class InnerObject fields differ too -- each column's full (year, gender,
        // class) signature is distinct, so each gets its own item.
        fn seg(year: &str, gender: &str, class: &str) -> KeySegment {
            KeySegment::Array(
                Arc::from("longevity"),
                Identifier::from_int_str(year),
                Arc::from("year"),
                Arc::new(KeySegment::InnerObject(
                    Identifier::from_string(gender),
                    Arc::from("gender"),
                    Arc::new(KeySegment::InnerObject(
                        Identifier::from_string(class),
                        Arc::from("class"),
                        Arc::new(KeySegment::Simple(Arc::from("value"))),
                    )),
                )),
            )
        }
        let segments = [
            (seg("2015", "female", "upperclass"), Value::from(87.9)),
            (seg("2015", "male", "upperclass"), Value::from(83.8)),
            (seg("2015", "female", "lowerclass"), Value::from(80.4)),
            (seg("2015", "male", "lowerclass"), Value::from(76.9)),
        ];
        let result = run(&segments);
        assert_eq!(
            result,
            serde_json::json!({"longevity": [
                {"year": 2015, "gender": "female", "class": "upperclass", "value": 87.9},
                {"year": 2015, "gender": "male",   "class": "upperclass", "value": 83.8},
                {"year": 2015, "gender": "female", "class": "lowerclass", "value": 80.4},
                {"year": 2015, "gender": "male",   "class": "lowerclass", "value": 76.9}
            ]})
        );
    }

    #[test]
    fn test_array_with_inner_objects_still_merges_when_full_signature_agrees() {
        // Two columns resolving to the exact same (year, gender, class) signature land
        // in the same item -- e.g. a duplicate/aliased source column -- last value wins
        // on the shared "value" field rather than creating a spurious second item.
        fn seg() -> KeySegment {
            KeySegment::Array(
                Arc::from("longevity"),
                Identifier::from_int_str("2015"),
                Arc::from("year"),
                Arc::new(KeySegment::InnerObject(
                    Identifier::from_string("female"),
                    Arc::from("gender"),
                    Arc::new(KeySegment::Simple(Arc::from("value"))),
                )),
            )
        }
        let result = run(&[(seg(), Value::from(87.9)), (seg(), Value::from(88.0))]);
        assert_eq!(
            result,
            serde_json::json!({"longevity": [
                {"year": 2015, "gender": "female", "value": 88.0}
            ]})
        );
    }
}
