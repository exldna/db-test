use serde::{
    Deserializer,
    de::{self, Visitor},
};

pub struct DurationVisitor;

impl<'de> Visitor<'de> for DurationVisitor {
    type Value = std::time::Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a number of seconds")
    }

    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(std::time::Duration::from_secs(value as u64))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(std::time::Duration::from_secs(value as u64))
    }

    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(std::time::Duration::from_secs(value as u64))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(std::time::Duration::from_secs(value))
    }
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<std::time::Duration, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_i32(DurationVisitor)
}
