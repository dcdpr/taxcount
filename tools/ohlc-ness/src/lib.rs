#![forbid(unsafe_code)]

use thiserror::Error;

pub mod consts;
mod fetch;
pub mod modes;

#[derive(Debug, Error)]
pub enum BucketError {
    #[error("Invalid bucket size. Try one of: `daily`, `hourly`")]
    Buckets,
}

#[derive(Clone, Copy, Debug)]
pub enum Buckets {
    Daily,
    Hourly,
}

impl TryFrom<String> for Buckets {
    type Error = BucketError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "daily" => Ok(Self::Daily),
            "hourly" => Ok(Self::Hourly),
            _ => Err(Self::Error::Buckets),
        }
    }
}

pub fn normalize_timestamp_ns(time: u64) -> u64 {
    // Normalize to nanosecond resolution
    if time < 1_000_000_000_000_000_000 {
        time * 1_000_000_000
    } else {
        time
    }
}

pub fn normalize_timestamp(time: u64) -> u64 {
    // Normalize to second resolution
    if time >= 1_000_000_000_000_000_000 {
        time / 1_000_000_000
    } else {
        time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_timestamp_ns() {
        let actual = 1_645_684_440;
        let expected = 1_645_684_440_000_000_000;
        assert_eq!(normalize_timestamp_ns(actual), expected);

        let actual = 1_645_684_440_815_857_143;
        let expected = 1_645_684_440_815_857_143;
        assert_eq!(normalize_timestamp_ns(actual), expected);
    }

    #[test]
    fn test_normalize_timestamp() {
        let actual = 1_645_684_440;
        let expected = 1_645_684_440;
        assert_eq!(normalize_timestamp(actual), expected);

        let actual = 1_645_684_440_815_857_143;
        let expected = 1_645_684_440;
        assert_eq!(normalize_timestamp(actual), expected);
    }
}
