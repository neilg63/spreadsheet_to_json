use indexmap::IndexMap;
use serde_json::Value;
use simple_string_patterns::SimpleMatch;
use std::{cmp::Ordering, fmt, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchMode {
    Cs,
    Ci,
    AlphaNum
}

/// A single column's match rule -- a static, reusable definition. Deliberately holds
/// no row: one `ColumnMatch` is built once (as part of a `RowOptionSet::filter_rules`
/// tree) and evaluated against every row in the file, so the row it's being checked
/// against has to be a parameter to `matches`, not state it owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMatch {
    path: Vec<Arc<str>>,
    condition: FilterMatch,
}

impl ColumnMatch {
    /// A single, flat top-level key -- the common case.
    pub fn new(key: impl Into<Arc<str>>, condition: FilterMatch) -> Self {
        ColumnMatch { path: vec![key.into()], condition }
    }

    /// A dot-path into nested output built via `KeySegment::Object` (e.g. `--keys
    /// "size_width:size.width"`) -- `&["size", "width"]` for `size.width`. Doesn't
    /// support a `"$"` array-wildcard segment the way `exclude_nested_path` does:
    /// removing a key from every array item is unconditional, but *matching* through
    /// an array raises a real question `exclude_nested_path` never had to answer --
    /// does the row match if *any* item satisfies the condition, or only if *all* of
    /// them do? Left out until that's a deliberate decision rather than a silent one;
    /// a `"$"` segment here is treated as a literal (and essentially always-absent) key.
    pub fn at_path(path: &[&str], condition: FilterMatch) -> Self {
        ColumnMatch { path: path.iter().map(|s| Arc::from(*s)).collect(), condition }
    }

    pub fn path(&self) -> &[Arc<str>] {
        &self.path
    }

    pub fn condition(&self) -> &FilterMatch {
        &self.condition
    }

    /// `None` if any segment of the path is missing, or a non-terminal segment isn't
    /// an object to descend into -- distinct from `Some(false)` (the value is present
    /// but doesn't match), so callers can tell "missing" from "didn't match" if needed.
    pub fn matches(&self, row: &IndexMap<String, Value>) -> Option<bool> {
        let (first, rest) = self.path.split_first()?;
        let mut value = row.get(first.as_ref())?;
        for segment in rest {
            value = value.as_object()?.get(segment.as_ref())?;
        }
        Some(self.condition.matches(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterMatch {
    Exact(Value, MatchMode),
    StartsWith(Value, MatchMode),
    EndsWith(Value, MatchMode),
    Contains(Value, MatchMode),
    Gte(Value),
    Gt(Value),
    Lte(Value),
    Lt(Value),
    Between(Value, Value),
    NotBetween(Value, Value),
    /// The row's (scalar) value equals one of these -- SQL `IN (...)`.
    OneOf(Vec<Value>),
    /// The row's value is itself an array, and it shares at least one element with
    /// this list -- e.g. a `PlainArray`-mapped "tags" column containing any of the
    /// given tags. Distinct from `OneOf`: `OneOf` compares a scalar row value against
    /// several candidates; `AnyOneOf` compares an *array* row value against several
    /// candidates, matching if their intersection is non-empty. `false` (not `None`,
    /// this is `matches`, not `ColumnMatch::matches`) if the row's value isn't an array.
    AnyOneOf(Vec<Value>),
    NotEmpty,
    Custom(CustomMatch),
}

impl FilterMatch {
    pub fn matches(&self, value: &Value) -> bool {
        match self {
            Self::Exact(v, mode) => match mode {
                MatchMode::Cs => v == value,
                MatchMode::Ci => v.
                    as_str().map_or(false, |s| s.equals_ci(value.as_str().unwrap_or(""))),
                MatchMode::AlphaNum => 
                    v.as_str()
                    .map_or(false, |s| s.equals_ci_alphanum(value.as_str().unwrap_or(""))),
            },
            Self::StartsWith(v, mode) => match mode {
                MatchMode::Cs => value
                    .as_str()
                    .map_or(false, |s| s.starts_with(v.as_str().unwrap_or(""))),
                MatchMode::Ci => value
                    .as_str()
                    .map_or(false, |s| s.starts_with_ci(v.as_str().unwrap_or(""))),
                MatchMode::AlphaNum => value
                    .as_str()
                    .map_or(false, |s| s.starts_with_ci_alphanum(v.as_str().unwrap_or(""))),
            },
            Self::EndsWith(v, mode) => match mode {
                MatchMode::Cs => value
                    .as_str()
                    .map_or(false, |s| s.ends_with(v.as_str().unwrap_or(""))),
                MatchMode::Ci => value
                    .as_str()
                    .map_or(false, |s| s.ends_with_ci(v.as_str().unwrap_or(""))),
                MatchMode::AlphaNum => value
                    .as_str()
                    .map_or(false, |s| s.ends_with_ci_alphanum(v.as_str().unwrap_or(""))),
            },
            Self::Contains(v, mode) => match mode {
                MatchMode::Cs => value
                    .as_str()
                    .map_or(false, |s| s.contains(v.as_str().unwrap_or(""))),
                MatchMode::Ci => value
                    .as_str()
                    .map_or(false, |s| s.contains_ci(v.as_str().unwrap_or(""))),
                MatchMode::AlphaNum => value
                    .as_str()
                    .map_or(false, |s| s.contains_ci_alphanum(v.as_str().unwrap_or(""))),
            },
            // Exact, literal comparison against `v` -- no partial-date/range expansion
            // (e.g. a bare "2026-06" meaning "anywhere in June") happens here. That's a
            // deliberate scope boundary: a partial literal represents a *range*, and
            // which boundary an operator should expand it to differs by operator (Lt/Gte
            // anchor to the range's start as-is; Gt/Lte need the *next* range's start,
            // with Lte flipping to strict Lt against it) -- see the design notes. Making
            // FilterMatch itself guess at that would tie leaf semantics to a judgment
            // call that belongs wherever a FilterCondition tree gets built from a literal
            // (e.g. a future --filter text parser), not to plain value comparison here.
            Self::Gte(v) => {
                ordered_values(value, v).is_some_and(|ordering| ordering != Ordering::Less)
            }
            Self::Gt(v) => {
                ordered_values(value, v).is_some_and(|ordering| ordering == Ordering::Greater)
            }
            Self::Lte(v) => {
                ordered_values(value, v).is_some_and(|ordering| ordering != Ordering::Greater)
            }
            Self::Lt(v) => {
                ordered_values(value, v).is_some_and(|ordering| ordering == Ordering::Less)
            }
            Self::NotEmpty => match value {
                Value::Null => false,
                Value::String(s) => !s.trim().is_empty(),
                Value::Array(a) => !a.is_empty(),
                Value::Object(o) => !o.is_empty(),
                _ => true,
            },
            Self::Between(lo, hi) => is_between(value, lo, hi),
            // Defined as the exact negation of Between (not independently re-derived via
            // < / > ), so a value that Between can't even compare (mismatched types)
            // correctly counts as "not between" here rather than silently disagreeing
            // with Between's own notion of "no match" at that same edge case.
            Self::NotBetween(lo, hi) => !is_between(value, lo, hi),
            // Plain Value equality, like Exact's Cs mode -- not routed through
            // ordered_values, which only knows how to compare (String, String) and
            // number-like pairs. Equality is strictly more general than ordering: it
            // has to work for Bool/Null/Array/Object too, which ordered_values would
            // silently treat as "never equal" (Bool::as_f64() is None, so a OneOf of
            // booleans could never match either boolean value).
            Self::OneOf(values) => values.iter().any(|v| v == value),
            Self::AnyOneOf(values) => match value {
                Value::Array(items) => items.iter().any(|item| values.contains(item)),
                _ => false,
            },
            Self::Custom(c) => c.matches(value),
        }
    }
}

pub type MatchFn = dyn Fn(&str, &Value) -> bool + Send + Sync + 'static;

#[derive(Clone)]
pub struct CustomMatch {
    name: Arc<str>,
    pattern: Arc<str>,
    func: Arc<MatchFn>,
}

impl CustomMatch {
    pub fn new<F>(name: impl Into<Arc<str>>, pattern: impl Into<Arc<str>>, func: F) -> Self
    where
        F: Fn(&str, &Value) -> bool + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            pattern: pattern.into(),
            func: Arc::new(func),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Cheap handle to the pattern — clone this rather than the whole filter
    /// if you only need the text.
    pub fn pattern_arc(&self) -> Arc<str> {
        Arc::clone(&self.pattern)
    }

    #[inline]
    pub fn matches(&self, value: &Value) -> bool {
        (self.func)(&self.pattern, value)
    }
}

impl fmt::Debug for CustomMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomMatch")
            .field("name", &self.name)
            .field("pattern", &self.pattern)
            .finish_non_exhaustive()
    }
}

impl PartialEq for CustomMatch {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.pattern == other.pattern
            && Arc::ptr_eq(&self.func, &other.func)
    }
}

impl Eq for CustomMatch {}



#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterCondition {
    Match(ColumnMatch),
    And(Vec<FilterCondition>),
    Or(Vec<FilterCondition>),
    /// Wraps an arbitrary sub-condition, not just a single `ColumnMatch` -- needed to
    /// express De Morgan's-style negation of a compound condition (`NOT (a AND b)`),
    /// not just "not this one field check". `Arc`, not `Box`, matching `KeySegment`'s
    /// existing convention in this crate: this tree gets cloned along with
    /// `RowOptionSet` (e.g. for a header-only peek), so cheap clones matter.
    Not(Arc<FilterCondition>),
}

impl FilterCondition {
    /// Evaluate the whole tree against one row. A `Match` whose key is entirely
    /// absent from `row` counts as "no match" (`ColumnMatch::matches` returning
    /// `None`) -- consistent with `FilterMatch`'s own "can't compare/no value ->
    /// no match" convention elsewhere (e.g. `Between` on mismatched types), rather
    /// than letting a missing column silently satisfy the filter.
    pub fn evaluate(&self, row: &IndexMap<String, Value>) -> bool {
        match self {
            Self::Match(column_match) => column_match.matches(row).unwrap_or(false),
            Self::And(conditions) => conditions.iter().all(|c| c.evaluate(row)),
            Self::Or(conditions) => conditions.iter().any(|c| c.evaluate(row)),
            Self::Not(inner) => !inner.evaluate(row),
        }
    }
}

/// Single source of truth for what "between" means, shared by `Between` and
/// `NotBetween` so the two can never independently drift apart at an edge case.
fn is_between(value: &Value, lo: &Value, hi: &Value) -> bool {
    ordered_values(value, lo).is_some_and(|ordering| ordering != Ordering::Less)
        && ordered_values(value, hi).is_some_and(|ordering| ordering != Ordering::Greater)
}

fn ordered_values(value: &Value, filter: &Value) -> Option<Ordering> {
    match (value, filter) {
        (Value::String(value), Value::String(filter)) => Some(value.cmp(filter)),
        _ => value
            .as_f64()
            .zip(filter.as_f64())
            .and_then(|(value, filter)| value.partial_cmp(&filter)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::{json, Value};

    fn row(pairs: &[(&str, Value)]) -> IndexMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn test_column_match_distinguishes_missing_key_from_non_matching_value() {
        let r = row(&[("engine_type", json!("diesel"))]);
        let present_no_match = ColumnMatch::new("engine_type", FilterMatch::Exact(json!("petrol"), MatchMode::Cs));
        let absent = ColumnMatch::new("colour", FilterMatch::Exact(json!("red"), MatchMode::Cs));

        assert_eq!(present_no_match.matches(&r), Some(false));
        assert_eq!(absent.matches(&r), None);
    }

    #[test]
    fn test_column_match_at_path_descends_into_a_nested_object() {
        let r: IndexMap<String, Value> = serde_json::from_value(json!({
            "size": {"width": 5, "height": 20}
        })).unwrap();
        let narrow = ColumnMatch::at_path(&["size", "width"], FilterMatch::Lt(json!(10)));
        let tall = ColumnMatch::at_path(&["size", "height"], FilterMatch::Lt(json!(10)));
        let missing = ColumnMatch::at_path(&["size", "depth"], FilterMatch::Lt(json!(10)));
        let non_object = ColumnMatch::at_path(&["size", "width", "extra"], FilterMatch::Lt(json!(10)));

        assert_eq!(narrow.matches(&r), Some(true));
        assert_eq!(tall.matches(&r), Some(false));
        assert_eq!(missing.matches(&r), None);
        // "width"'s own value (5) isn't an object, so descending further is None, not
        // a panic or a silent false
        assert_eq!(non_object.matches(&r), None);
    }

    #[test]
    fn test_any_one_of_matches_array_values_intersecting_the_list() {
        let with_codes = row(&[("codes", json!(["us", "ca"]))]);
        let no_overlap = row(&[("codes", json!(["fr", "de"]))]);
        let scalar = row(&[("codes", json!("us"))]);

        let condition = FilterMatch::AnyOneOf(vec![json!("us"), json!("gb")]);
        assert!(condition.matches(&with_codes["codes"]));
        assert!(!condition.matches(&no_overlap["codes"]));
        // AnyOneOf requires the row's own value to be an array -- a scalar never
        // matches, even if it equals one of the candidates (that's OneOf's job)
        assert!(!condition.matches(&scalar["codes"]));
    }

    #[test]
    fn test_filter_condition_evaluates_nested_and_or() {
        // (start_date >= '2025-01-01' AND engine_type = 'petrol') OR engine_type = 'electric'
        let condition = FilterCondition::Or(vec![
            FilterCondition::And(vec![
                FilterCondition::Match(ColumnMatch::new("start_date", FilterMatch::Gte(json!("2025-01-01")))),
                FilterCondition::Match(ColumnMatch::new("engine_type", FilterMatch::Exact(json!("petrol"), MatchMode::Cs))),
            ]),
            FilterCondition::Match(ColumnMatch::new("engine_type", FilterMatch::Exact(json!("electric"), MatchMode::Cs))),
        ]);

        let recent_petrol = row(&[("start_date", json!("2025-06-01")), ("engine_type", json!("petrol"))]);
        let old_petrol = row(&[("start_date", json!("2020-06-01")), ("engine_type", json!("petrol"))]);
        let old_electric = row(&[("start_date", json!("2020-06-01")), ("engine_type", json!("electric"))]);
        let old_diesel = row(&[("start_date", json!("2020-06-01")), ("engine_type", json!("diesel"))]);

        assert!(condition.evaluate(&recent_petrol));
        assert!(!condition.evaluate(&old_petrol));
        assert!(condition.evaluate(&old_electric));
        assert!(!condition.evaluate(&old_diesel));
    }

    #[test]
    fn test_filter_condition_not_negates_a_compound_condition() {
        let compound = FilterCondition::And(vec![
            FilterCondition::Match(ColumnMatch::new("a", FilterMatch::Exact(json!(1), MatchMode::Cs))),
            FilterCondition::Match(ColumnMatch::new("b", FilterMatch::Exact(json!(2), MatchMode::Cs))),
        ]);
        let negated = FilterCondition::Not(Arc::new(compound));

        assert!(!negated.evaluate(&row(&[("a", json!(1)), ("b", json!(2))])));
        assert!(negated.evaluate(&row(&[("a", json!(1)), ("b", json!(99))])));
    }

    #[test]
    fn test_filter_condition_missing_key_counts_as_no_match_not_a_pass() {
        let condition = FilterCondition::Match(ColumnMatch::new("missing", FilterMatch::NotEmpty));
        assert!(!condition.evaluate(&row(&[("present", json!("x"))])));
    }

    #[test]
    fn test_ordered_filters_compare_strings_lexicographically() {
        let value = Value::String("beta".to_string());

        assert!(FilterMatch::Gte(Value::String("beta".to_string())).matches(&value));
        assert!(FilterMatch::Gt(Value::String("alpha".to_string())).matches(&value));
        assert!(FilterMatch::Lte(Value::String("beta".to_string())).matches(&value));
        assert!(FilterMatch::Lt(Value::String("gamma".to_string())).matches(&value));
        assert!(!FilterMatch::Lt(Value::String("alpha".to_string())).matches(&value));
    }

    #[test]
    fn test_ordered_filters_do_not_compare_mixed_types() {
        let value = Value::String("10".to_string());

        assert!(!FilterMatch::Gte(json!(5)).matches(&value));
        assert!(!FilterMatch::Lt(json!(20)).matches(&value));
    }

    #[test]
    fn test_one_of_matches_by_plain_equality_not_ordering() {
        // Bool/Null/Array/Object have no `ordered_values` branch at all -- OneOf must
        // still match them via plain Value equality, not silently treat them as
        // never-equal the way routing through ordering would.
        assert!(FilterMatch::OneOf(vec![json!(true), json!(false)]).matches(&json!(true)));
        assert!(!FilterMatch::OneOf(vec![json!(false)]).matches(&json!(true)));
        assert!(FilterMatch::OneOf(vec![Value::Null]).matches(&Value::Null));
        assert!(FilterMatch::OneOf(vec![json!([1, 2])]).matches(&json!([1, 2])));

        // still works for the ordinary string/number case
        assert!(FilterMatch::OneOf(vec![json!("petrol"), json!("electric")]).matches(&json!("electric")));
        assert!(!FilterMatch::OneOf(vec![json!("petrol"), json!("electric")]).matches(&json!("diesel")));
    }

    #[test]
    fn test_not_between_is_the_exact_negation_of_between() {
        let lo = json!(10);
        let hi = json!(20);

        for candidate in [json!(5), json!(10), json!(15), json!(20), json!(25)] {
            let between = FilterMatch::Between(lo.clone(), hi.clone()).matches(&candidate);
            let not_between = FilterMatch::NotBetween(lo.clone(), hi.clone()).matches(&candidate);
            assert_eq!(not_between, !between, "diverged for candidate {candidate}");
        }

        // Between can't compare a string value against numeric bounds -- it correctly
        // says "no match", and NotBetween, being its exact negation, correctly says
        // the opposite ("not between" is true for something that isn't comparable to
        // the range at all), rather than the two silently agreeing on "false".
        let incomparable = json!("not a number");
        assert!(!FilterMatch::Between(lo.clone(), hi.clone()).matches(&incomparable));
        assert!(FilterMatch::NotBetween(lo, hi).matches(&incomparable));
    }
}

