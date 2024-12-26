
pub trait RoundDecimal {
    fn round_decimal(&self, decimals: u8) -> Self;
}

impl RoundDecimal for f64 {
    fn round_decimal(&self, decimals: u8) -> Self {
        let multiplier = 10.0f64.powi(decimals as i32);
        (*self * multiplier).round() / multiplier
    }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_round_decimal() {
      let sample_number = 3.14159265359;
      assert_eq!(sample_number.round_decimal(2), 3.14);

      assert_eq!(sample_number.round_decimal(3), 3.142);

      assert_eq!(sample_number.round_decimal(4), 3.1416);
  }

}