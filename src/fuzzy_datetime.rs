use chrono::{format::ParseErrorKind, NaiveDateTime};
use simple_string_patterns::{CharGroupMatch, CharType, SimplContainsType, ToSegments};

use crate::error::GenericError;

pub fn fuzzy_to_datetime(dt: &str) -> Result<NaiveDateTime, GenericError> {
  if let Some(formatted_str) = fuzzy_to_datetime_string(dt) {
    NaiveDateTime::parse_from_str(&formatted_str, "%Y-%m-%dT%H:%M:%S%.3fZ").map_err(|e| {
        match e.kind() {
            ParseErrorKind::BadFormat => GenericError("bad_format"),
            _ => GenericError("invalid_date_string")
        }
    })
  } else {
    Err(GenericError("invalid_date_string"))
  }
}

pub fn fuzzy_to_date_string(dt: &str) -> Option<String> {
  if let Some((date_str, _t_str, _ms_tz)) = fuzzy_to_date_string_with_time(dt) {
    if !date_str.is_empty() {
      Some(date_str)
    } else {
      None
    }
  } else {
    None
  }
}

/// convert a date-time-like string to a valid ISO 8601-compatbile string
pub fn fuzzy_to_date_string_with_time(dt: &str) -> Option<(String, String, String)> {
	let (dt_base, milli_tz) = dt.to_start_end(".");
	let clean_dt = dt_base.replace("T", " ").trim().to_string();
	let mut dt_parts = clean_dt.split_whitespace();
	let date_part = dt_parts.next().unwrap_or("0000-01-01");
	if date_part.contains_type(CharType::Alpha) {
			return None;
	}
	let time_part = dt_parts.next().unwrap_or("00:00:00");

	let d_parts: Vec<&str> = date_part.split('-').collect();
	let mut date_parts: Vec<&str> = d_parts.into_iter().filter(|&n| n.is_digits_only()).collect();
	if date_parts.len() < 1 {
			return None;
	}
	while date_parts.len() < 3 {
			date_parts.push("01");
	}
	let month = date_parts[1].parse::<u8>().unwrap_or(0);
	if month < 1 || month > 12 {
		return None;
	}
	let day = date_parts[2].parse::<u8>().unwrap_or(0);
	if month < 1 || day > 31 {
		return None;
	}
	let formatted_date = format!("{}-{:02}-{:02}", date_parts[0], month, day);

	Some((formatted_date, time_part.to_string(), milli_tz))
}

/// convert a date-time-like string to a valid ISO 8601-compatbile string
pub fn fuzzy_to_datetime_string(dt: &str) -> Option<String> {
	fuzzy_to_datetime_string_opts(dt, 'T', true)
}

/// convert a date-time-like string to a valid ISO 8601-compatbile string
/// dt: the date-time string
/// separator: the separator between the date and time parts
/// add_z: whether to add 'Z' timezone indicator
pub fn fuzzy_to_datetime_string_opts(dt: &str, separator: char, add_z: bool) -> Option<String> {
  if let Some((formatted_date, time_part, ms_tz)) = fuzzy_to_date_string_with_time(dt) {
		let t_parts: Vec<&str> = time_part.split(':').collect();
    if let Some(&first) = t_parts.get(0) {
        if !first.is_digits_only() {
            return None;
        }
    }
    let mut time_parts: Vec<u8> = t_parts.into_iter()
		.filter(|&n| n.is_digits_only())
		.map(|tp| tp.parse::<u8>().unwrap_or(0))
		.collect();

    while time_parts.len() < 3 {
        time_parts.push(0);
    }
		let hrs = time_parts[0];
		if hrs > 23 {
			return None;
		}
		let mins = time_parts[1];
		if mins > 59 {
			return None;
		}
		let secs = time_parts[2];
		if secs > 59 {
			return None;
		}
    let formatted_time = format!("{:02}:{:02}:{:02}", hrs, mins, secs);
    let tz_suffix = if add_z {
      let max_len = if ms_tz.len() > 3 {
        3
      } else {
        ms_tz.len()
      };
      let ms = ms_tz[0..max_len].parse::<u16>().unwrap_or(0);
      format!(".{:03}Z", ms)
    } else {
      "".to_string()
    };
    let formatted_str = format!("{}{}{}{}", formatted_date, separator, formatted_time, tz_suffix);
    if !formatted_str.is_empty() {
      Some(formatted_str)
    } else {
      None
    }
	} else {
		None
	}
}

pub fn is_datetime_like(text: &str) -> bool {
    fuzzy_to_datetime_string(text).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fuzzy_dates() {
        let sample_1 = "2001-apple";
        assert!(fuzzy_to_datetime(sample_1).is_err());
        assert_eq!(fuzzy_to_datetime_string(sample_1), None);

        let sample_2 = "1876-08-29 17:15";
        assert!(fuzzy_to_datetime(sample_2).is_ok());

				// correct sample datetime
        let sample_3 = "2023-8-29 19:34:39";
        assert_eq!(
            fuzzy_to_datetime_string(sample_3),
            Some("2023-08-29T19:34:39.000Z".to_string())
        );

				// Correct date-only string
        let sample_4 = "2023-9-10";
        assert_eq!(
            fuzzy_to_date_string(sample_4),
            Some("2023-09-10".to_string())
        );
				// time-only strings should not be valid
        let sample_5 = "10:10:10";
        assert_eq!(
            fuzzy_to_datetime_string(sample_5),
            None
        );

        // datetime with extra milliseconds and timezone
        let sample_3 = "2023-08-29T19:34:39.678Z";
        assert_eq!(
            fuzzy_to_datetime_string(sample_3),
            Some(sample_3.to_string())
        );
    }

    #[test]
    fn test_is_datetime_like() {
        assert!(is_datetime_like("2023-10-10T10:10:10"));
        assert!(is_datetime_like("2023-10-10 10:10:10"));
        assert!(is_datetime_like("2023-10-10"));
        assert!(!is_datetime_like("10:10:10"));
        assert!(!is_datetime_like("invalid-date"));
        assert!(!is_datetime_like("2023-10-10Tinvalid"));
    }
}