
pub trait RoundDecimal {
    fn round_decimal(&self, decimals: u8) -> Self;
}

impl RoundDecimal for f64 {
    fn round_decimal(&self, decimals: u8) -> Self {
        let multiplier = 10.0f64.powi(decimals as i32);
        (*self * multiplier).round() / multiplier
    }
}
