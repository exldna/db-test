use db_test_compare::{backends::*, *};
use db_test_model::list_data_files;

use std::time::Duration;

const BENCH_NAME: &str = "insert_bulk";
const BENCH_GROUP_NAME: &str = const_format::formatc!("bench.{BENCH_NAME}");

fn insert_bulk_bencher<B>(b: &mut criterion::Bencher, context: &Context<B>, bench_input: &B::Input)
where
    B: Backend<Input = InsertBulkInput>,
{
    b.to_async(&context.runtime).iter_batched(
        || {
            let prepare = async {
                let bench = context.backend.prepare(bench_input).await;
                bench.unwrap()
            };
            context.block(prepare)
        },
        async |bench| bench.run().await,
        // We hold the running container
        criterion::BatchSize::PerIteration,
    );
}

fn insert_bulk_bench_group<'a, B>(c: &mut criterion::Criterion, context: &Context<B>)
where
    B: Backend<Input = InsertBulkInput>,
{
    let mut group = c.benchmark_group(BENCH_GROUP_NAME);
    for (i, file_path) in list_data_files().unwrap().enumerate() {
        // group.throughput(criterion::Throughput::Elements(items_count));
        group.bench_function(criterion::BenchmarkId::new(BENCH_NAME, i), |b| {
            let bench_input = InsertBulkInput {
                file_path: file_path.clone(),
            };
            insert_bulk_bencher(b, context, &bench_input);
        });
    }
    group.finish();
}

fn insert_bulk_benchmark<B>(c: &mut criterion::Criterion)
where
    B: Backend<Input = InsertBulkInput>,
{
    let context = Context::<B>::new().unwrap();
    let _enter = context.runtime.enter();
    insert_bulk_bench_group(c, &context);
}

criterion::criterion_group! {
    name = insert_bulk;
    config = criterion::Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(60))
        .noise_threshold(0.05);
    targets =
        insert_bulk_benchmark<redis::insert_bulk::RedisInsertBulk>,
        // insert_bulk_benchmark<PostgresInsertBulk>,
        // insert_bulk_benchmark<SqliteInsertBulk>,
}

criterion::criterion_main!(insert_bulk);
