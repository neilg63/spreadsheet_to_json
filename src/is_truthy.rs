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

pub fn is_truthy_custom(txt: &str, opts: &[(bool, &str, bool, bool)], use_defaults: bool, empty_is_false: bool) -> Option<bool> {
  // Will return the first matched letter sequence
  let txt = txt.trim();
  for &(is_true, letters, case_sensitive, starts_with) in opts {
    if starts_with {
      if case_sensitive {
        if txt.starts_with(letters) {
          return Some(is_true)
        }
      } else {
        if txt.starts_with_ci_alphanum(letters) {
          return Some(is_true)
        }
      }
    } else {
      if case_sensitive {
        if txt == letters {
          return Some(is_true)
        }
      } else {
        if txt.to_lowercase() == letters.to_lowercase() {
          return Some(is_true);
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
    let custom_flags = [
      (true, "si", false, false),
      (true, "vero", false, false),
      (false, "no", false, false),
      (false, "falso", false, false),
    ];
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