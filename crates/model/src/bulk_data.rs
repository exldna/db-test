use std::borrow::Cow;
use std::collections::btree_map::Keys;

use num_rational::Ratio;

use rand::distr::{Alphanumeric, Distribution, SampleString};
use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};

/// User address
#[derive(Debug, Clone, PartialEq)]
pub struct UserAddress(String);

impl UserAddress {
    const ADDRESS_LENGTH: usize = 26;

    fn new_random(rng: &mut impl Rng) -> Self {
        let sample = Alphanumeric.sample_string(rng, Self::ADDRESS_LENGTH);
        UserAddress(sample)
    }
}

/// Transaction timesatmp
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TransactionTimestamp(u64);

impl TransactionTimestamp {
    const BOUNDARY: u64 = 1742817035;

    fn new_random(rng: &mut impl Rng) -> Self {
        TransactionTimestamp(rng.random_range(0..Self::BOUNDARY))
    }
}

/// Transaction hash
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionHash(String);

impl TransactionHash {
    const ID_LENGTH: usize = 64;

    fn new_random(rng: &mut impl Rng) -> Self {
        let sample = Hexadecimal.sample_string(rng, Self::ID_LENGTH);
        TransactionHash(sample)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction(UserAddress, TransactionTimestamp, TransactionHash);

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

enum UserRating {
    Major, // Users with maximum transactions count
    Minor,
    Other,
}

pub struct BulkDataGenerator {
    rng: SmallRng,
}

impl BulkDataGenerator {
    const MAJOR_USERS: Ratio<u64> = Ratio::new_raw(1, 100);
    const MINOR_USERS: Ratio<u64> = Ratio::new_raw(1, 10);

    pub fn new(x: u64) -> Self {
        BulkDataGenerator {
            rng: SmallRng::from_os_rng(),
        }
    }

    fn get_users_count(x: u64) -> u64 {
        const MN: Ratio<u64> = BulkDataGenerator::MINOR_USERS;
        const MJ: Ratio<u64> = BulkDataGenerator::MAJOR_USERS;

        let numer = 8 * x * ((MN - MJ).to_integer());

        numer
    }

    fn random_ratio(&mut self, ratio: Ratio<u32>) -> bool {
        self.rng.random_ratio(*ratio.numer(), *ratio.denom())
    }
}

impl Iterator for BulkDataGenerator {
    type Item = Transaction;

    fn next(&mut self) -> Option<Self::Item> {
        None
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
