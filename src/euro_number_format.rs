
/// Detect if a numeric string uses the European format with , as the decimal separator and dots as thousand separators
/// If only one comma is present, *enforce_euro_mode* will treat the comma as a decimal separator.
/// Otherwise it will be assumed to be a thousand separator
pub fn is_euro_number_format(txt: &str, enforce_euro_mode: bool) -> bool {
    let chs = txt.char_indices();
    let mut num_indices = 0;
    let mut dot_pos: Option<usize> = None;
    let mut num_dots = 0;
    let mut num_commas = 0;
    let mut comma_pos: Option<usize> = None;
    for (index, ch) in chs {
        match ch {
            '.' => {
                if dot_pos.is_none() {
                    dot_pos = Some(index);
                }
                num_dots += 1;
            }
            ',' => {
                if comma_pos.is_none() {
                    comma_pos = Some(index);
                }
                num_commas += 1;
            },
            _ => ()
        }
        num_indices += 1; // count indices here to avoid cloning above
    }
    if let Some(d_pos) = dot_pos {
        if let Some(c_pos) = comma_pos {
            d_pos < c_pos
        } else {
            // if it only has one dot only interpreet as decimal separator if enforce_euro_mode is true
            num_dots > 1 || enforce_euro_mode
        }
    } else {
        // no dots
        if let Some(c_pos) = comma_pos {
            if num_commas > 1 {
                false
            } else {
                // with only one comma, assume it's a decimal separator
                // if it is not exactly 4 positions from the right or if enforce
                num_indices - c_pos != 4 || enforce_euro_mode
            }
        } else {
            false
        }

    }
}



#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_lnumber_format_1() {
        let sample = "1.256";
        assert_eq!(is_euro_number_format(sample, false), false);
    }

    #[test]
    fn test_lnumber_format_2() {
        let sample = "1,256";
        assert_eq!(is_euro_number_format(sample, false), false);
    }

    #[test]
    fn test_lnumber_format_3() {

        let sample = "12,56";
        
        assert_eq!(is_euro_number_format(sample, false), true);
    }

    #[test]
    fn test_lnumber_format_4() {

        let sample = "1,256.67";
        
        assert_eq!(is_euro_number_format(sample, false), false);
    }

    #[test]
    fn test_lnumber_format_5() {

        let sample = "1.256,67";
        
        assert_eq!(is_euro_number_format(sample, false), true);
    }
}