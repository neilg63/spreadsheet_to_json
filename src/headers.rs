
use heck::ToSnakeCase;

use crate::Column;

pub fn to_radix_groups_with_offset_option(n: u32, radix: u32, index_offset: bool) -> Vec<u32> {
    let mut result = Vec::new();
    let mut num = n;
    let mut not_first = false;
    while num > 0 {
        let offset = if not_first && index_offset {
            1
        } else {
            0
        };
        result.push(num % radix - offset);
        num /= radix;
        not_first = true;
    }

    result.reverse(); // Because we've pushed the least significant digit first
    result
}

#[allow(dead_code)]
pub fn to_radix_groups(n: u32, radix: u32) -> Vec<u32> {
    to_radix_groups_with_offset_option(n, radix, false)
}


pub fn to_radix_groups_offset(n: u32, radix: u32) -> Vec<u32> {
    to_radix_groups_with_offset_option(n, radix, true)
}

pub fn to_letter(index: u32) -> char {
    char::from_u32(97 + index).unwrap_or(' ')
}

pub fn to_head_key(index: usize) -> String {
    if index < 1 {
        'a'.to_string()
    } else {
        let groups = to_radix_groups_offset(index as u32, 26);
        groups.into_iter().map(|ci| to_letter(ci)).collect::<String>() 
    }
}

pub fn build_header_keys(first_row: &[String], columns: &[Column]) -> Vec<String> {
let mut h_index = 0;
    let num_cells = first_row.len();
    let mut headers: Vec<String> = vec![];
    let num_pop_header_cells = first_row.len();
    let add_custom_headers = num_pop_header_cells >= num_cells;
    for h_row in first_row.to_owned() {
        let sn = h_row.to_snake_case();
        if let Some(col) = columns.get(h_index) {
            headers.push(col.key.to_string());
        } else {
            if add_custom_headers && sn.len() > 0 {
                headers.push(sn);
            } else {
                headers.push(to_head_key(h_index));
            }
        }
        h_index += 1;
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_letter() {

        assert_eq!(to_letter(5), 'f');

        assert_eq!(to_letter(25), 'z');
    }

    #[test]
    fn test_radix_groups() {

        assert_eq!(to_radix_groups(27, 26), vec![1, 1]);


        assert_eq!(to_radix_groups(24, 26), vec![24]);

        assert_eq!(to_radix_groups(56, 26), vec![2, 4]);
    }

    #[test]
    fn test_radix_groups_offset() {

        assert_eq!(to_radix_groups_offset(27, 26), vec![0, 1]);

        assert_eq!(to_radix_groups_offset(26, 26), vec![0, 0]);


        assert_eq!(to_radix_groups_offset(24, 26), vec![24]);

        assert_eq!(to_radix_groups_offset(56, 26), vec![1, 4]);
    }

    #[test]
    fn test_cell_letters() {

        assert_eq!(to_head_key(26), "aa");
    }
}