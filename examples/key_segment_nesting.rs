//! Demonstrates `KeySegment`'s non-linear options -- everything beyond the flat,
//! one-column-to-one-field case (`KeySegment::Simple`, which is what a plain `--keys`
//! rename already produces). Each section below builds a small CSV in memory, maps a
//! couple of its columns through a different `KeySegment` shape, runs it through the
//! real `process_spreadsheet_direct` pipeline, and prints the resulting JSON.
//!
//! Run with: cargo run --example key_segment_nesting

use spreadsheet_to_json::{
    Column, DateTimeMode, Format, Identifier, KeySegment, OptionSet, process_spreadsheet_direct,
};
use std::sync::Arc;

/// Writes `content` to a temp CSV file and returns its path, so each demo below can
/// build `OptionSet::new(path)` against a real file the same way a caller would.
fn write_csv(filename: &str, content: &str) -> String {
    let path = std::env::temp_dir().join(filename);
    std::fs::write(&path, content).expect("failed to write temp CSV");
    path.to_string_lossy().to_string()
}

fn run_and_print(title: &str, opts: &OptionSet) {
    println!("\n=== {title} ===");
    let result = process_spreadsheet_direct(opts).expect("processing failed");
    println!("{}", serde_json::to_string_pretty(&result.to_vec()).unwrap());
}

fn main() {
    object_nesting();
    plain_array();
    array_with_inner_object();
    excluded();
    from_json();
}

/// `KeySegment::Object` -- descend into (creating if needed) a nested object, then
/// place the value inside it. Two columns sharing the same container key merge into
/// one nested object rather than each creating their own.
fn object_nesting() {
    let path = write_csv(
        "keyseg_object.csv",
        "name,weight,height\nalice,60,165\nbob,80,180\n",
    );
    let mut opts = OptionSet::new(&path);

    let mut weight_col = Column::from_source_key_with_format("weight", None, Format::Integer, None, DateTimeMode::Full, false);
    weight_col.key = Some(KeySegment::Object(Arc::from("measurements"), Arc::new(KeySegment::Simple(Arc::from("weight")))));

    let mut height_col = Column::from_source_key_with_format("height", None, Format::Integer, None, DateTimeMode::Full, false);
    height_col.key = Some(KeySegment::Object(Arc::from("measurements"), Arc::new(KeySegment::Simple(Arc::from("height")))));

    opts.rows.columns = vec![weight_col, height_col];
    run_and_print("Object: weight/height -> measurements.{weight,height}", &opts);
}

/// `KeySegment::PlainArray` -- push each column's own value directly into a named
/// array, as a bare scalar (no wrapping object, no discriminator). The natural fit for
/// `file_1`/`file_2`/`file_3`-style repeated columns; order follows column position.
fn plain_array() {
    let path = write_csv(
        "keyseg_plain_array.csv",
        "id,file_1,file_2,file_3\n1,report.pdf,invoice.pdf,receipt.pdf\n",
    );
    let mut opts = OptionSet::new(&path);

    let cols = ["file_1", "file_2", "file_3"]
        .iter()
        .map(|src| {
            let mut col = Column::from_source_key_with_format(src, None, Format::Auto, None, DateTimeMode::Full, false);
            col.key = Some(KeySegment::PlainArray(Arc::from("files")));
            col
        })
        .collect();

    opts.rows.columns = cols;
    run_and_print("PlainArray: file_1/file_2/file_3 -> files: [...]", &opts);
}

/// `KeySegment::Array` + `KeySegment::InnerObject` -- the richest case. `Array`
/// finds-or-creates an item in a named array by matching a typed `Identifier`;
/// `InnerObject` inlines further literal-valued fields onto that *same* item rather
/// than nesting them, so several columns can flatten onto one array entry. Two columns
/// sharing a year land in the same item only if their *entire* chain of identifiers
/// agrees -- see `KeySegment::Array`'s own doc comment for exactly how that's decided.
fn array_with_inner_object() {
    let path = write_csv(
        "keyseg_array_inner_object.csv",
        "country,longevity_2015_female,longevity_2015_male,longevity_2025_female,longevity_2025_male\n\
         USA,87.9,83.8,88.7,84.5\n",
    );
    let mut opts = OptionSet::new(&path);

    // Each column's KeySegment fully resolves at config time: find-or-create the
    // longevity[] item where year == <this column's year>, then inline "gender" onto
    // that same item, then insert the cell's own value under "value".
    fn column_for(source_key: &str, year: i64, gender: &str) -> Column {
        let mut col = Column::from_source_key_with_format(source_key, None, Format::Float, None, DateTimeMode::Full, false);
        col.key = Some(KeySegment::Array(
            Arc::from("longevity"),
            Identifier::from_int(year),
            Arc::from("year"),
            Arc::new(KeySegment::InnerObject(
                Identifier::from_string(gender),
                Arc::from("gender"),
                Arc::new(KeySegment::Simple(Arc::from("value"))),
            )),
        ));
        col
    }

    opts.rows.columns = vec![
        column_for("longevity_2015_female", 2015, "female"),
        column_for("longevity_2015_male", 2015, "male"),
        column_for("longevity_2025_female", 2025, "female"),
        column_for("longevity_2025_male", 2025, "male"),
    ];
    run_and_print(
        "Array + InnerObject: longevity_<year>_<gender> -> longevity: [{year, gender, value}, ...]",
        &opts,
    );
}

/// `KeySegment::Excluded` -- drop a column from output entirely, rather than emitting
/// it under some key. Useful for a column that's derivable (a total, a checksum) and
/// doesn't need to round-trip into the JSON.
fn excluded() {
    let path = write_csv(
        "keyseg_excluded.csv",
        "sku,price,internal_margin\nSKU001,19.99,0.35\n",
    );
    let mut opts = OptionSet::new(&path);

    let mut margin_col = Column::from_source_key_with_format("internal_margin", None, Format::Float, None, DateTimeMode::Full, false);
    margin_col.key = Some(KeySegment::Excluded);

    opts.rows.columns = vec![margin_col];
    run_and_print("Excluded: internal_margin is dropped entirely", &opts);
}

/// The same nesting as `object_nesting`, but built entirely from JSON via
/// `Column::from_json` / `OptionSet::override_columns` -- the path a frontend UI or Web
/// API would actually use, with no `KeySegment` construction in Rust at all.
fn from_json() {
    let path = write_csv(
        "keyseg_from_json.csv",
        "name,weight,height\nalice,60,165\n",
    );
    let opts = OptionSet::new(&path).override_columns(&[
        serde_json::json!({
            "source_key": "weight",
            "format": "integer",
            "key": {"type": "object", "key": "measurements", "next": "weight"}
        }),
        serde_json::json!({
            "source_key": "height",
            "format": "integer",
            "key": {"type": "object", "key": "measurements", "next": "height"}
        }),
    ]);
    run_and_print("Column::from_json: same Object nesting, built from a JSON payload", &opts);
}
