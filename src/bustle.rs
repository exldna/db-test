use std::{sync::Arc, sync::Barrier, time::Duration};

use rand::prelude::*;
use tracing::{debug, info, info_span};

#[derive(Clone, Copy, Debug)]
pub struct Mix {
    pub read: u8,
    pub insert: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct Workload {
    mix: Mix,
    initial_cap_log2: u8,
    prefill: f64,
    operations: f64,
    threads: usize,
    seed: Option<[u8; 32]>,
}

pub trait Collection: Send + Sync + 'static {
    type Handle: CollectionHandle;

    fn with_capacity(capacity: usize) -> Self;

    fn pin(&self) -> Self::Handle;
}

pub trait CollectionHandle {
    type Key: From<u64>;

    fn get(&mut self, key: &Self::Key);

    fn insert(&mut self, key: &Self::Key);
}

#[derive(Debug, Clone)]
pub struct Measurement {
    pub total_ops: u64,
    pub spent: Duration,
    pub throughput: f64,
    pub latency: Duration,
}

impl Workload {
    pub fn new(threads: usize, mix: Mix) -> Self {
        Self {
            mix,
            initial_cap_log2: 25,
            prefill: 0.0,
            operations: 0.75,
            threads,
            seed: None,
        }
    }

    pub fn initial_capacity_log2(&mut self, capacity: u8) -> &mut Self {
        self.initial_cap_log2 = capacity;
        self
    }

    pub fn prefill_fraction(&mut self, fraction: f64) -> &mut Self {
        assert!(fraction >= 0.0);
        assert!(fraction <= 1.0);
        self.prefill = fraction;
        self
    }

    pub fn operations(&mut self, multiple: f64) -> &mut Self {
        assert!(multiple >= 0.0);
        self.operations = multiple;
        self
    }

    #[allow(clippy::cognitive_complexity)]
    pub fn run_silently<T: Collection>(&self) -> Measurement
    where
        <T::Handle as CollectionHandle>::Key: Send + std::fmt::Debug,
    {
        assert_eq!(
            self.mix.read + self.mix.insert,
            100,
            "mix fractions do not add up to 100%"
        );

        let initial_capacity = 1 << self.initial_cap_log2;
        let total_ops = (initial_capacity as f64 * self.operations) as usize;

        let seed = self.seed.unwrap_or_else(rand::random);
        let mut rng: rand::rngs::SmallRng = rand::SeedableRng::from_seed(seed);

        // NOTE: it'd be nice to include std::intrinsics::type_name::<T> here
        let span = info_span!("benchmark", mix = ?self.mix, threads = self.threads);
        let _guard = span.enter();
        debug!(initial_capacity, total_ops, ?seed, "workload parameters");

        info!("generating operation mix");
        let mut op_mix = Vec::with_capacity(100);
        op_mix.append(&mut vec![Operation::Read; usize::from(self.mix.read)]);
        op_mix.append(&mut vec![Operation::Insert; usize::from(self.mix.insert)]);
        op_mix.shuffle(&mut rng);

        info!("generating key space");
        let prefill = (initial_capacity as f64 * self.prefill) as usize;
        let max_insert_ops = total_ops.div_ceil(100) * usize::from(self.mix.insert);
        let insert_keys = std::cmp::max(initial_capacity, max_insert_ops) + prefill;
        let insert_keys_per_thread = insert_keys.div_ceil(self.threads).next_power_of_two();

        let mut generators = Vec::new();
        for _ in 0..self.threads {
            let mut thread_seed = [0u8; 32];
            rng.fill_bytes(&mut thread_seed[..]);
            generators.push(std::thread::spawn(move || {
                let mut rng: rand::rngs::SmallRng = rand::SeedableRng::from_seed(thread_seed);
                let mut keys: Vec<<T::Handle as CollectionHandle>::Key> =
                    Vec::with_capacity(insert_keys_per_thread);
                keys.extend((0..insert_keys_per_thread).map(|_| rng.next_u64().into()));
                keys
            }));
        }
        let keys: Vec<_> = generators
            .into_iter()
            .map(|jh| jh.join().unwrap())
            .collect();

        info!("constructing initial table");
        let table = Arc::new(T::with_capacity(initial_capacity));

        // And fill it
        let prefill_per_thread = prefill / self.threads;
        let mut prefillers = Vec::new();
        for keys in keys {
            let table = Arc::clone(&table);
            prefillers.push(std::thread::spawn(move || {
                let mut table = table.pin();
                for key in &keys[0..prefill_per_thread] {
                    table.insert(key);
                }
                keys
            }));
        }
        let keys: Vec<_> = prefillers
            .into_iter()
            .map(|jh| jh.join().unwrap())
            .collect();

        info!("start workload mix");
        let ops_per_thread = total_ops / self.threads;
        let op_mix = Arc::new(op_mix.into_boxed_slice());
        let barrier = Arc::new(Barrier::new(self.threads + 1));
        let mut mix_threads = Vec::with_capacity(self.threads);
        for keys in keys {
            let table = Arc::clone(&table);
            let op_mix = Arc::clone(&op_mix);
            let barrier = Arc::clone(&barrier);
            mix_threads.push(std::thread::spawn(move || {
                let mut table = table.pin();
                mix(
                    &mut table,
                    &keys,
                    &op_mix,
                    ops_per_thread,
                    prefill_per_thread,
                    barrier,
                )
            }));
        }

        barrier.wait();
        let start = std::time::Instant::now();
        barrier.wait();
        let spent = start.elapsed();

        let _samples: Vec<_> = mix_threads
            .into_iter()
            .map(|jh| jh.join().unwrap())
            .collect();

        let avg = spent / total_ops as u32;
        info!(?spent, ops = total_ops, ?avg, "workload mix finished");

        let total_ops = total_ops as u64;
        let threads = self.threads as u32;

        Measurement {
            total_ops,
            spent,
            throughput: total_ops as f64 / spent.as_secs_f64(),
            latency: Duration::from_nanos((spent * threads).as_nanos() as u64 / total_ops),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Read,
    Insert,
}

fn mix<H: CollectionHandle>(
    tbl: &mut H,
    keys: &[H::Key],
    op_mix: &[Operation],
    ops: usize,
    prefilled: usize,
    barrier: Arc<Barrier>,
) where
    H::Key: std::fmt::Debug,
{
    // Invariant: erase_seq <= insert_seq
    // Invariant: insert_seq < num_keys
    let n_keys = keys.len();
    let mut insert_seq = prefilled;
    let mut find_seq = 0;

    // We're going to use a very simple LCG to pick random keys.
    // We want it to be _super_ fast so it doesn't add any overhead.
    assert!(n_keys.is_power_of_two());
    assert!(n_keys > 4);
    assert_eq!(op_mix.len(), 100);
    let a = n_keys / 2 + 1;
    let c = n_keys / 4 - 1;
    let find_seq_mask = n_keys - 1;

    // The elapsed time is measured by the lifetime of `workload_scope`.
    let workload_scope = scopeguard::guard(barrier, |barrier| {
        barrier.wait();
    });
    workload_scope.wait();

    for (i, op) in (0..ops.div_ceil(op_mix.len()))
        .flat_map(|_| op_mix.iter())
        .enumerate()
    {
        if i == ops {
            break;
        }

        match op {
            Operation::Read => {
                tbl.get(&keys[find_seq]);
                find_seq = (a * find_seq + c) & find_seq_mask;
            }
            Operation::Insert => {
                tbl.insert(&keys[insert_seq]);
                insert_seq += 1;
            }
        }
    }
}
