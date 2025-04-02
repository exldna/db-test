use std::borrow::Cow;

use num_rational::Ratio;

use rand::distr::{Alphanumeric, Distribution, SampleString};
use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};

/// User address
#[derive(Debug, Clone, PartialEq)]
pub struct UserAddr(String);

impl UserAddr {
    const ADDRESS_LENGTH: usize = 26;

    fn new_random(rng: &mut impl Rng) -> Self {
        let sample = Alphanumeric.sample_string(rng, Self::ADDRESS_LENGTH);
        UserAddr(sample)
    }
}

/// Transaction timesatmp
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timestamp(u64);

impl Timestamp {
    const BOUNDARY: u64 = 1742817035;

    fn new_random(rng: &mut impl Rng) -> Self {
        Timestamp(rng.random_range(0..Self::BOUNDARY))
    }
}

/// Transaction hash
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionId(String);

impl TransactionId {
    const ID_LENGTH: usize = 64;

    fn new_random(rng: &mut impl Rng) -> Self {
        let sample = Hexadecimal.sample_string(rng, Self::ID_LENGTH);
        TransactionId(sample)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction(UserAddr, Timestamp, TransactionId);

impl Transaction {
    pub fn serialize_csv<W>(&self, writer: &mut csv::Writer<W>) -> anyhow::Result<()>
    where
        W: std::io::Write,
    {
        writer.write_record(&[
            self.0.0.as_bytes(),
            self.1.0.to_string().as_bytes(),
            self.2.0.as_bytes(),
        ])?;
        Ok(())
    }
}

pub struct BulkDataGenerator {
    rng: SmallRng,
    major_pool: Vec<UserAddr>,
}

impl BulkDataGenerator {
    const MAJOR_USERS: Ratio<u32> = Ratio::new_raw(1, 100);
    const MAJOR_TRANSACTIONS: Ratio<u32> = Ratio::new_raw(50, 100);

    pub fn new() -> Self {
        BulkDataGenerator {
            rng: SmallRng::from_os_rng(),
            major_pool: Vec::new(),
        }
    }

    fn random_ratio(&mut self, ratio: Ratio<u32>) -> bool {
        self.rng.random_ratio(*ratio.numer(), *ratio.denom())
    }

    fn peek_user_addr(&mut self) -> Cow<'_, UserAddr> {
        let mut user_addr = None;
        if self.random_ratio(Self::MAJOR_TRANSACTIONS) {
            let major_user = self.major_pool.choose(&mut self.rng);
            user_addr = major_user.map(|major_user| Cow::Borrowed(major_user));
        }
        if let Some(user_addr) = user_addr {
            user_addr
        } else {
            let random_user = UserAddr::new_random(&mut self.rng);
            Cow::Owned(random_user)
        }
    }
}

impl Iterator for BulkDataGenerator {
    type Item = Transaction;

    fn next(&mut self) -> Option<Self::Item> {
        if self.random_ratio(Self::MAJOR_USERS) {
            let major_user_addr = UserAddr::new_random(&mut self.rng);
            self.major_pool.push(major_user_addr);
        }
        let user_addr = self.peek_user_addr().into_owned();
        let timestamp = Timestamp::new_random(&mut self.rng);
        let id = TransactionId::new_random(&mut self.rng);
        Some(Transaction(user_addr, timestamp, id))
    }
}

struct Hexadecimal;

impl Distribution<u8> for Hexadecimal {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u8 {
        const HEX_DIGITS_CHARSET: &[u8; 16] = b"0123456789abcdef";

        let var = rng.next_u32() >> (32 - 4);
        HEX_DIGITS_CHARSET[var as usize]
    }
}

impl SampleString for Hexadecimal {
    fn append_string<R: Rng + ?Sized>(&self, rng: &mut R, string: &mut String, len: usize) {
        unsafe {
            let v = string.as_mut_vec();
            v.extend(self.sample_iter(rng).take(len));
        }
    }
}
