use std::fmt::Debug;
use std::iter;

use bustle::*;
use structopt::StructOpt;

use crate::{adapters::*, record::Record, workloads};

#[derive(Debug, StructOpt)]
pub struct Options {
    #[structopt(short, long)]
    pub workload: workloads::WorkloadKind,
    #[structopt(short, long, default_value = "1")]
    pub operations: f64,
    #[structopt(long)]
    pub threads: Option<Vec<u32>>,
    #[structopt(long)]
    pub skip: Vec<String>,
    #[structopt(long)]
    pub csv: bool,
    #[structopt(long)]
    pub csv_no_headers: bool,
}

type Handler = Box<dyn FnMut(&str, u32, &Measurement)>;

fn case<C>(name: &str, options: &Options, handler: &mut Handler)
where
    C: Collection,
    <C::Handle as CollectionHandle>::Key: Send + Debug,
{
    if options.skip.iter().find(|s| s == &name).is_some() {
        println!("-- {} [skipped]", name);
        return;
    } else {
        println!("-- {}", name);
    }

    let gen_threads = || {
        let n = num_cpus::get();

        match n {
            0..=10 => (1..=n as u32).collect(),
            11..=16 => iter::once(1)
                .chain((0..=n as u32).step_by(2).skip(1))
                .collect(),
            _ => iter::once(1)
                .chain((0..=n as u32).step_by(4).skip(1))
                .collect(),
        }
    };

    let threads = options
        .threads
        .as_ref()
        .cloned()
        .unwrap_or_else(gen_threads);

    for n in &threads {
        let m = workloads::create(options, *n).run_silently::<C>();
        handler(name, *n, &m);
    }

    println!();
}

fn run(options: &Options, h: &mut Handler) {
    // case::<MdbxTable>("std:sync::Arc<libmdbx::Database>", options, h);
    case::<RocksDbTable>("std:sync::Arc<rocksdb::DB>", options, h);
}

pub fn bench(options: &Options) {
    println!("== {:?}", options.workload);

    let mut handler = if options.csv {
        let mut wr = csv::WriterBuilder::new()
            .has_headers(!options.csv_no_headers)
            .from_writer(std::io::stderr());

        Box::new(move |name: &str, n, m: &Measurement| {
            wr.serialize(Record {
                name: name.into(),
                total_ops: m.total_ops,
                threads: n,
                spent: m.spent,
                throughput: m.throughput,
                latency: m.latency,
            })
            .expect("cannot serialize");
            wr.flush().expect("cannot flush");
        }) as Handler
    } else {
        Box::new(|_: &str, n, m: &Measurement| {
            eprintln!(
                "total_ops={}\tthreads={}\tspent={:.1?}\tlatency={:?}\tthroughput={:.0}op/s",
                m.total_ops, n, m.spent, m.latency, m.throughput,
            );
        }) as Handler
    };

    run(options, &mut handler);
}
