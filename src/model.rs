pub struct UserAddress(String);

impl From<u64> for UserAddress {
    fn from(value: u64) -> Self {
        UserAddress(format!("user{}", value))
    }
}

impl UserAddress {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

pub const VALUE_DATA: [u8] = b"3c26012ed49b73fd4cdf32e561f7c3f9088d02ea37a44d23485088385a7e463b";
