use std::{fmt::Debug, str::FromStr};

use crate::bustle::*;
use crate::bench::Options;

#[derive(Debug)]
pub enum WorkloadKind {
    ReadHeavy,
    RapidGrow,
}

impl FromStr for WorkloadKind {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ReadHeavy" => Ok(Self::ReadHeavy),
            "RapidGrow" => Ok(Self::RapidGrow),
            _ => Err("unknown workload"),
        }
    }
}

fn read_heavy(threads: u32) -> Workload {
    let mix = Mix{
            read: 95,
            insert: 5,
        
    };

    *Workload::new(threads as usize, mix)
        .initial_capacity_log2(25)
        .prefill_fraction(0.75)
}

fn rapid_grow(threads: u32) -> Workload {
    let mix = Mix{
        read: 20,
            insert: 80,
    };

    *Workload::new(threads as usize, mix)
        .initial_capacity_log2(25)
        .prefill_fraction(0.0)
}

pub(crate) fn create(options: &Options, threads: u32) -> Workload {
    let mut workload = match options.workload {
        WorkloadKind::ReadHeavy => read_heavy(threads),
        WorkloadKind::RapidGrow => rapid_grow(threads),
    };

    workload.operations(options.operations);
    workload
}
