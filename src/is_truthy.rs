use std::sync::Arc;

use simple_string_patterns::*;

/// Only match string representations of booleans, integers or floats
pub fn is_truthy_core(txt: &str, empty_is_false: bool) -> Option<bool> {
  let test_str = txt.trim().to_lowercase();
  match test_str.as_str() {
    "" => {
      if empty_is_false {
        Some(false)
      } else {
        None
      }
    },
    "0" | "-1" | "false" => Some(false),
    "1" | "2" | "true" => Some(true),
    _ => if test_str.is_digits_only() {
      if let Some(int_val) = test_str.to_first_number::<u8>() {
        Some(int_val > 0)
      } else {
        None
      }
    } else {
      None
    }
  }
}

/// match boolean with core boolean/number + common English-like words
pub fn is_truthy_standard(txt: &str, empty_is_false: bool) -> Option<bool> {
  let test_str = txt.trim().to_lowercase();
  if let Some(core_match) = is_truthy_core(txt, empty_is_false) {
    Some(core_match)
  } else {
    match test_str.as_str() {
      "no" | "not" | "none" | "n" | "f" => Some(false),
      "ok" | "okay" |"y" | "yes" | "t" => Some(true),
      _ => None
    }
  }
}

/// Validate a string cell from an array of truthy options with patterns that may be true or false
/// if unmatched return None, otherwise Some(true) or Some(false)
#[allow(dead_code)]
pub fn is_truthy_custom(txt: &str, opts: &[TruthyOption], use_defaults: bool, empty_is_false: bool) -> Option<bool> {
  // Will return the first matched letter sequence
  let txt = txt.trim();
  for opt in opts {
    let letters = opt.sample();
    if opt.starts_with {
      if opt.case_sensitive {
        if txt.starts_with(&letters) {
          return Some(opt.is_true)
        }
      } else {
        if txt.starts_with_ci_alphanum(&letters) {
          return Some(opt.is_true)
        }
      }
    } else {
      if opt.case_sensitive {
        if txt == letters {
          return Some(opt.is_true)
        }
      } else {
        if txt.to_lowercase() == letters.to_lowercase() {
          return Some(opt.is_true);
        }
      }
    }
  }
  if use_defaults {
    is_truthy_core(txt, empty_is_false)
  } else {
    None
  }
}

/// Truth Option that may be case-sensitive and match either the start or anywhere within a string
/// It's assumed truthy field use consistent naming conventions, but this allows some flexibility
/// without using full regular expressions
#[derive(Debug)]
pub struct TruthyOption {
  pub is_true: bool,
  pub pattern: Arc<str>,
  pub case_sensitive: bool,
  pub starts_with: bool
}

impl TruthyOption {
  pub fn new(is_true: bool, pattern: &str, case_sensitive: bool, starts_with: bool) -> Self {
    Self {
      is_true,
      pattern: Arc::from(pattern),
      case_sensitive,
      starts_with
    }
  }

  pub fn sample(&self) -> String {
    self.pattern.to_string()
  }
}

/// convert a custom string setting into a full TruthyOptiuon
/// e.g. truthy|ok,good|failed,bad will be translated into two true options (ok or good) and two false options (failed and bad)
/// case_sensitive and starts_with are applied globally
#[allow(dead_code)]
pub fn split_truthy_custom_option_str(custom_str: &str, case_sensitive: bool, starts_with: bool) -> Vec<TruthyOption> {
  let parts = custom_str.to_segments(",");
  let mut matchers:Vec<TruthyOption> = vec![];
  if parts.len() > 2 {
    if let Some(first) = parts.get(0) {
      if first.starts_with_ci_alphanum("tru") {
        let yes_parts = parts.get(1).unwrap_or(&"".to_string()).to_segments("|");
        let no_parts = parts.get(2).unwrap_or(&"".to_string()).to_segments("|");
        for match_str in yes_parts {
          matchers.push(TruthyOption::new(true, &match_str, case_sensitive, starts_with));
        }
        for match_str in no_parts {
          matchers.push(TruthyOption::new(false, &match_str, case_sensitive, starts_with));
        }
      }
    }
  }
  matchers
}



#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_truthy_1() {
      let sample = "1";
      assert_eq!(is_truthy_standard(sample, false), Some(true));

      let sample = "0";
      assert_eq!(is_truthy_standard(sample, false), Some(false));

      let sample = "false";
      assert_eq!(is_truthy_standard(sample, false), Some(false));
  }

  #[test]
  fn test_truthy_2() {
      let sample = "n";
      assert_eq!(is_truthy_standard(sample, false), Some(false));

      let sample = "Ok";
      assert_eq!(is_truthy_standard(sample, false), Some(true));

      let sample = "false";
      assert_eq!(is_truthy_standard(sample, false), Some(false));
  }

  #[test]
  fn test_truthy_custom() {

    let custom_setting_str = "truthy,si|vero,no|falso";
    let custom_flags = split_truthy_custom_option_str(custom_setting_str, false, false);

    // yes will be neither true nor false, because we're using custom true/false flags
    let sample = "yes";
    assert_eq!(is_truthy_custom(sample, &custom_flags, true, false), None);

    // will skip normal English "false" and only use "falso" or "no" for false
    let sample = "false";
    assert_eq!(is_truthy_custom(sample, &custom_flags, false, false), None);

    let sample = "si";
    assert_eq!(is_truthy_custom(sample, &custom_flags, true, true), Some(true));
  }
}