use alphanumeric::IsNumeric;
use fuzzy_datetime::{iso_fuzzy_to_date_string, iso_fuzzy_to_datetime_string};

use crate::OptionSet;

/// How many rows from the top of a sheet/file to sample when guessing the header and
/// first-data row. Generous enough to cover realistic title/notes preambles without
/// reading arbitrarily deep into a file just to make a guess.
pub const DETECT_SAMPLE_SIZE: usize = 20;

/// How many rows below a header candidate to compare it against for the last-resort
/// cell-length signal (see `looks_like_labels`).
const LENGTH_COMPARISON_SAMPLE: usize = 3;

/// A header candidate's average cell length must be under this fraction of the
/// comparison data rows' average to count as confidently label-like. Deliberately
/// conservative (labels are usually *much* shorter than sentence-like data, not just
/// somewhat shorter) since this is a weaker, last-resort signal only consulted when no
/// type-based signal (numeric/boolean/date) exists anywhere in the sample.
const LENGTH_CONFIDENCE_RATIO: f64 = 0.7;

/// Resolved header/data row indices for a read. `header_index: None` means detection
/// found no confident evidence a header row exists at all (see `looks_like_labels`) --
/// callers should fall back to A1/C01-style field names (the same as `--omit-header`)
/// rather than deriving them from a row that's actually data, and treat `data_index`
/// (which then points at the *first* row of the table, not the row after some header)
/// as where capture begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DetectedRows {
  pub header_index: Option<usize>,
  pub data_index: usize,
}

/// Number of populated (non-blank, after trimming) cells in a row -- the core signal
/// used to tell a real table row (header or data) apart from a title/notes row, which
/// typically only populates one or two leading cells regardless of the table's real
/// width.
fn row_width(row: &[String]) -> usize {
  row.iter().filter(|c| !c.trim().is_empty()).count()
}

/// Average character length of a row's populated cells -- the last-resort signal for
/// distinguishing header labels from data when there's no type-based signal to lean on
/// (see `looks_like_labels`).
fn avg_cell_length(row: &[String]) -> f64 {
  let populated: Vec<&String> = row.iter().filter(|c| !c.trim().is_empty()).collect();
  if populated.is_empty() {
    return 0.0;
  }
  let total: usize = populated.iter().map(|c| c.trim().chars().count()).sum();
  total as f64 / populated.len() as f64
}

/// A bare 4-digit integer in a plausible calendar year range (e.g. a "2020" column
/// header in a year-by-year table) -- numeric, but a legitimate label, not a data
/// signal. Checked ahead of both the numeric and date checks below so it's excluded
/// from both, rather than counting *against* a genuine header row.
fn looks_like_year(cell: &str) -> bool {
  cell.len() == 4
    && cell.chars().all(|c| c.is_ascii_digit())
    && cell.parse::<u32>().is_ok_and(|n| (1900..=2100).contains(&n))
}

fn is_boolean_like(cell: &str) -> bool {
  matches!(cell.to_lowercase().as_str(), "true" | "false" | "yes" | "no")
}

/// A clearly formatted date/datetime string (contains separators like "-", "/", ":" --
/// not just a bare number). Deliberately not applied to purely numeric strings: the
/// underlying fuzzy date parser treats *any* bare integer as a plausible bare year
/// (e.g. it happily reads "54000" as the year 54000), which would misfire on ordinary
/// numeric data like a salary or ID figure.
fn is_date_like(cell: &str) -> bool {
  if cell.chars().all(|c| c.is_ascii_digit()) {
    return false;
  }
  iso_fuzzy_to_date_string(cell).is_some() || iso_fuzzy_to_datetime_string(cell).is_some()
}

/// Whether a single cell looks like genuine data content rather than a column label.
/// Headers are almost always plain text; data cells are commonly numeric, boolean, or
/// date/datetime-shaped instead -- boolean and date values in particular are a much
/// more reliable "this is data" signal than plain numeric content, since header labels
/// essentially never take those forms (unlike numbers, which legitimately show up in
/// headers as e.g. year columns -- see `looks_like_year`).
fn is_data_signal(cell: &str) -> bool {
  let trimmed = cell.trim();
  if trimmed.is_empty() || looks_like_year(trimmed) {
    return false;
  }
  is_boolean_like(trimmed) || trimmed.is_numeric() || is_date_like(trimmed)
}

/// Whether at least half of a row's populated cells look like data (numeric, boolean,
/// or date/datetime) rather than column labels -- used to rule out treating a genuine
/// data row as the header.
fn looks_like_data(row: &[String]) -> bool {
  let populated: Vec<&String> = row.iter().filter(|c| !c.trim().is_empty()).collect();
  if populated.is_empty() {
    return false;
  }
  let signal_count = populated.iter().filter(|c| is_data_signal(c)).count();
  signal_count * 2 >= populated.len()
}

/// Last-resort signal for purely textual content (e.g. spreadsheets used for content
/// migration or translation, with no numbers/booleans/dates anywhere) -- content
/// migration is a common enough use of spreadsheets that `looks_like_data`'s
/// type-based check has nothing to work with there at all. Header labels ("region",
/// "key", "english") are almost always noticeably *shorter* than genuine data content
/// (full sentences, names, descriptions), even when that data is also plain text, so a
/// header candidate whose cells are much shorter than the comparison rows below it is
/// treated as confidently label-like. This is deliberately weaker/more conservative
/// than `looks_like_data` -- a real header can legitimately be longer than short data
/// (e.g. "International Sales Revenue (USD, Fiscal Year)" above bare numbers) -- so
/// it's a last resort, not a primary signal.
fn looks_like_labels(candidate: &[String], comparison_rows: &[&Vec<String>]) -> bool {
  if comparison_rows.is_empty() {
    // Nothing to compare against -- no evidence either way, so don't claim confidence.
    return false;
  }
  let candidate_len = avg_cell_length(candidate);
  let data_len: f64 =
    comparison_rows.iter().map(|r| avg_cell_length(r)).sum::<f64>() / comparison_rows.len() as f64;
  data_len > 0.0 && candidate_len < data_len * LENGTH_CONFIDENCE_RATIO
}

/// Best-guess header/data row indices from a sample of a sheet's/file's raw row text,
/// for when neither is explicitly configured.
///
/// The core signal is column width: a title or notes row (e.g. "Sales 2025", a paragraph
/// of explanatory text) almost always populates only one or two leading cells, while both
/// the header row and every data row populate roughly the table's full column count. This
/// finds the table's real column count as the *maximum* width observed among rows with 2+
/// populated cells -- not the most common ("mode") width -- because the header always
/// labels every column, while individual data rows commonly leave optional fields blank
/// (e.g. a "website" or "LinkedIn profile" column that's empty for some rows), so the mode
/// width often reflects the data's *incompleteness* rather than the table's true shape.
/// The header candidate is the first row that reaches this maximum (fully populated); the
/// first row *after* it that's merely wide enough to plausibly be a table row (at least
/// half the header's width, allowing for blank optional fields) marks the start of data.
///
/// The candidate is then checked against two signals, weakest last:
/// - `looks_like_data`: if it's mostly numeric/boolean/date content, it's data, not
///   labels (falls back to assuming row 0 is the header, the previous simple default).
/// - `looks_like_labels`: only consulted when *no* row in the whole sample has any
///   type-based data signal at all (purely textual content) -- if the candidate isn't
///   confidently shorter than the rows below it, there's no reliable evidence a header
///   exists, and `header_index` comes back `None` (see `DetectedRows`) rather than
///   guessing wrong and losing the candidate row as data.
pub(crate) fn detect_header_and_data_rows(sample_rows: &[Vec<String>]) -> DetectedRows {
  const FALLBACK: DetectedRows = DetectedRows { header_index: Some(0), data_index: 1 };
  if sample_rows.is_empty() {
    return FALLBACK;
  }

  let widths: Vec<usize> = sample_rows.iter().map(|r| row_width(r)).collect();

  let Some(target_width) = widths.iter().copied().filter(|&w| w >= 2).max() else {
    return FALLBACK;
  };

  let Some(header_index) = widths.iter().position(|&w| w == target_width) else {
    return FALLBACK;
  };
  if looks_like_data(&sample_rows[header_index]) {
    // The first fully-populated row looks like data, not column labels -- more likely
    // there's no header at all than that a numeric header exists.
    return FALLBACK;
  }

  // Data rows only need to be wide enough to plausibly be a table row, not fully
  // populated like the header -- blank optional fields are normal.
  let min_data_width = target_width.div_ceil(2).max(2);
  let matching_after: Vec<usize> = widths
    .iter()
    .enumerate()
    .skip(header_index + 1)
    .filter(|&(_, &w)| w >= min_data_width)
    .map(|(i, _)| i)
    .collect();

  // A header that's uniquely *wider* than every comparison row (it labels every
  // column; data rows below it leave some optional fields blank) is already strong,
  // purely structural evidence on its own -- skip the fragile length check entirely
  // rather than let it second-guess a candidate the width gap already confirms. Cell
  // length is genuinely unreliable here: a header can easily average *longer* than
  // data that mixes very short values (e.g. country codes) with very long ones (e.g.
  // URLs), even though the header is obviously still the header.
  let comparison_max_width = matching_after
    .iter()
    .take(LENGTH_COMPARISON_SAMPLE)
    .map(|&i| widths[i])
    .max()
    .unwrap_or(0);
  let width_gap_confirms_header = target_width > comparison_max_width;

  let any_type_signal = sample_rows.iter().any(|r| r.iter().any(|c| is_data_signal(c)));
  if !any_type_signal && !width_gap_confirms_header {
    let comparison_rows: Vec<&Vec<String>> = matching_after
      .iter()
      .take(LENGTH_COMPARISON_SAMPLE)
      .map(|&i| &sample_rows[i])
      .collect();
    if !looks_like_labels(&sample_rows[header_index], &comparison_rows) {
      // Purely textual content, and the candidate isn't confidently shorter than what
      // follows -- no reliable evidence of a real header, so the candidate itself is
      // treated as the first row of data instead of being consumed as a label row.
      return DetectedRows { header_index: None, data_index: header_index };
    }
  }

  let data_index = matching_after.first().copied().unwrap_or(header_index + 1);
  DetectedRows { header_index: Some(header_index), data_index }
}

/// Resolves the effective header/data row configuration for a read. Runs auto-detection
/// via `sample` -- called lazily, only when actually needed -- when `detect_header` is
/// opted into (off by default for direct library use; see the field's own doc) and both
/// `header_row`/`data_row_index` are unset and headers aren't omitted; otherwise resolves
/// explicit/default values the same way `OptionSet::header_row_index`/
/// `first_data_row_index` already do, just combined into one call.
pub(crate) fn resolve_header_and_data_rows<F: FnOnce() -> Vec<Vec<String>>>(
  opts: &OptionSet,
  sample: F,
) -> DetectedRows {
  if opts.detect_header && !opts.omit_header && opts.header_row.is_none() && opts.data_row_index.is_none() {
    return detect_header_and_data_rows(&sample());
  }
  // Inlined rather than going through OptionSet::first_data_row_index(), which returns
  // None (an ambiguous "nothing to do" signal, not "0") in exactly this omit_header-only
  // case -- that method's early return exists for a different caller (direct library use
  // that wants to skip its own gating check entirely), not this always-resolve context.
  let header_row_index = opts.header_row_index();
  let default_start = if opts.omit_header { header_row_index } else { header_row_index + 1 };
  let data_index = match opts.data_row_index {
    Some(requested) if requested >= header_row_index => requested,
    _ => default_start,
  };
  let header_index = if opts.omit_header { None } else { Some(header_row_index) };
  DetectedRows { header_index, data_index }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|s| s.to_string()).collect()
  }

  fn found(header_index: usize, data_index: usize) -> DetectedRows {
    DetectedRows { header_index: Some(header_index), data_index }
  }

  #[test]
  fn test_detects_header_with_notes_row_between_header_and_data() {
    // The example from the user: a title row, a proper 3-column header, an
    // explanatory-text row that only fills one cell, then real data.
    let sample = vec![
      row(&["Sales 2025"]),
      row(&["region", "team size", "revenue"]),
      row(&["long explanation about the data"]),
      row(&["west", "12", "923456"]),
      row(&["east", "7", "817285"]),
    ];
    assert_eq!(detect_header_and_data_rows(&sample), found(1, 3));
  }

  #[test]
  fn test_detects_header_with_no_gap() {
    // The common case: header immediately followed by data, no notes rows at all.
    let sample = vec![
      row(&["sku", "qty"]),
      row(&["SKU001", "10"]),
      row(&["SKU002", "20"]),
    ];
    assert_eq!(detect_header_and_data_rows(&sample), found(0, 1));
  }

  #[test]
  fn test_falls_back_when_sample_is_empty() {
    assert_eq!(detect_header_and_data_rows(&[]), found(0, 1));
  }

  #[test]
  fn test_falls_back_when_no_row_has_two_or_more_cells() {
    // Every row is single-column -- no width signal to work with.
    let sample = vec![row(&["a"]), row(&["b"]), row(&["c"])];
    assert_eq!(detect_header_and_data_rows(&sample), found(0, 1));
  }

  #[test]
  fn test_falls_back_when_first_wide_row_looks_numeric() {
    // No title row this time -- data starts immediately and happens to be the first
    // row wide enough to be "the table". Numeric content rules it out as a header.
    let sample = vec![row(&["100", "200"]), row(&["300", "400"])];
    assert_eq!(detect_header_and_data_rows(&sample), found(0, 1));
  }

  #[test]
  fn test_year_column_headers_are_not_mistaken_for_data() {
    // A header row of bare 4-digit years ("2020", "2021", "2022") is numeric-looking
    // but a completely normal, legitimate header for a year-by-year table -- it must
    // not be rejected the way genuinely numeric data rows are.
    let sample = vec![
      row(&["region", "2020", "2021", "2022"]),
      row(&["west", "12000", "13500", "14200"]),
      row(&["east", "9800", "10100", "11000"]),
    ];
    assert_eq!(detect_header_and_data_rows(&sample), found(0, 1));
  }

  #[test]
  fn test_boolean_and_date_cells_are_a_stronger_data_signal_than_plain_numbers() {
    // A row mixing plain text with a boolean and a date/datetime value: booleans and
    // dates essentially never appear in header labels, so even one such cell should be
    // enough to mark this as data rather than a header, unlike a bare year number.
    let sample = vec![
      row(&["name", "active", "joined"]),
      row(&["Alice", "true", "2024-01-15"]),
      row(&["Bob", "false", "2023-06-02"]),
    ];
    assert_eq!(detect_header_and_data_rows(&sample), found(0, 1));
  }

  #[test]
  fn test_large_numbers_are_not_misread_as_dates() {
    // Regression guard for the underlying fuzzy date parser's own quirk: it treats any
    // bare integer as a plausible year (e.g. it parses "54000" as the year 54000 AD),
    // which would otherwise make an ordinary numeric data row look "date-like" via a
    // completely different code path than the plain-numeric check. is_date_like must
    // not fire on bare digit strings at all -- only on separator-containing ones. A
    // title row pushes the real header off index 0, so a wrong (data-like) verdict on
    // the header candidate would show up as a different result than (1, 2).
    let sample = vec![
      row(&["Payroll Report"]),
      row(&["employee", "salary"]),
      row(&["Alice", "54000"]),
      row(&["Bob", "923456"]),
    ];
    assert_eq!(detect_header_and_data_rows(&sample), found(1, 2));
  }

  #[test]
  fn test_purely_textual_header_is_still_detected_via_length() {
    // No numeric/boolean/date content anywhere (a content-migration/translation
    // spreadsheet), but the header candidate's cells are much shorter than the data
    // rows below it -- confidently label-like even without a type signal.
    let sample = vec![
      row(&["Product Descriptions - EN to FR Migration"]),
      row(&["key", "english", "french"]),
      row(&["Translator notes: verify context before translating"]),
      row(&["welcome_msg", "Welcome to our store", "Bienvenue dans notre magasin"]),
      row(&["goodbye_msg", "Thank you for visiting", "Merci de votre visite"]),
    ];
    assert_eq!(detect_header_and_data_rows(&sample), found(1, 3));
  }

  #[test]
  fn test_header_is_the_fully_populated_row_not_the_most_common_width() {
    // The user's example: a header where every column is labeled, but each data row
    // has different optional fields left blank (website, LinkedIn profile), so no
    // single width repeats as often as the *incomplete* data rows' widths do. The old
    // "most common width" signal would have picked one of the data rows (width 5,
    // appearing twice) over the header (width 6, appearing once) -- this must not
    // happen: the header is the fully-populated row, and blank optional fields are
    // still recognized as data.
    let sample = vec![
      row(&["Authors"]),
      row(&["first name", "last name", "country code", "bio", "website", "linkedin profile"]),
      row(&["Explanatory notes"]),
      row(&["Jane", "Blogs", "ca", "Lorem ipsum", "", ""]),
      row(&["Joe", "Doe", "au", "Lorem ipsum", "https://www.joedoe.com", ""]),
      row(&["Giovanna", "Rossi", "it", "Lorem ipsum", "", "https://www.linkedin.com/in/giovanna-rossi/"]),
    ];
    assert_eq!(detect_header_and_data_rows(&sample), found(1, 3));
  }

  #[test]
  fn test_headerless_purely_textual_file_reports_no_header_found() {
    // No title row, no numeric content, and the first (and only) row shape repeats --
    // there's no length gap between "candidate" and what follows because both rows are
    // genuinely data. header_index must come back None so the caller doesn't consume
    // this row as a label row and lose it as data.
    let sample = vec![
      row(&["welcome_msg", "Welcome to our store", "Bienvenue dans notre magasin"]),
      row(&["goodbye_msg", "Thank you for visiting", "Merci de votre visite"]),
    ];
    let result = detect_header_and_data_rows(&sample);
    assert_eq!(result.header_index, None);
    assert_eq!(result.data_index, 0);
  }
}
