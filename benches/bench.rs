use criterion::{criterion_group, criterion_main, Criterion};
use kv_store::KVS;
use std::sync::{Arc, Barrier};
use std::thread;
use rand::Rng;

fn bench_read_mostly(c: &mut Criterion) {
    const NUM_THREADS: usize = 8;
    const NUM_KEYS: usize = 10000;
    const INIT_VAL_SIZE: usize = 1024; // 1KB

    let hash_table = Arc::new(KVS::new());
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    // Prepopulate the table
    for i in 0..NUM_KEYS {
        let key = format!("{:08}", i);  // 8B string
        let value = "a".repeat(INIT_VAL_SIZE);
        hash_table.put(key, value);
    }

    c.bench_function("read_mostly", |b| {
        b.iter(|| {
            let handles = (0..NUM_THREADS)
                .map(|_| {
                    let hash_table_clone = Arc::clone(&hash_table);
                    let barrier_clone = barrier.clone();

                    thread::spawn(move || {
                        barrier_clone.wait();

                        for _ in 0..NUM_KEYS {
                            if rand::random::<f32>() < 0.95 {
                                let key_idx = rand::thread_rng().gen_range(0..NUM_KEYS);
                                let key = format!("{:08}", key_idx);
                                hash_table_clone.get(&key);
                            } else {
                                let key_idx = rand::thread_rng().gen_range(0..NUM_KEYS);
                                let key = format!("{:08}", key_idx);
                                let value = "b".repeat(INIT_VAL_SIZE);
                                hash_table_clone.put(key, value);
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

fn bench_write_mostly(c: &mut Criterion) {
    const NUM_THREADS: usize = 8;
    const NUM_KEYS: usize = 10000;
    const INIT_VAL_SIZE: usize = 1024; // 1KB

    let hash_table = Arc::new(KVS::new());
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    // Prepopulate the table
    for i in 0..NUM_KEYS {
        let key = format!("{:08}", i);  // 8B string
        let value = "a".repeat(INIT_VAL_SIZE);
        hash_table.put(key, value);
    }

    c.bench_function("write_mostly", |b| {
        b.iter(|| {
            let handles = (0..NUM_THREADS)
                .map(|_| {
                    let hash_table_clone = Arc::clone(&hash_table);
                    let barrier_clone = barrier.clone();

                    thread::spawn(move || {
                        barrier_clone.wait();

                        for _ in 0..NUM_KEYS {
                            let prob = rand::random::<f32>();

                            if prob < 0.70 {
                                let key_idx = rand::thread_rng().gen_range(0..NUM_KEYS);
                                let key = format!("{:08}", key_idx);
                                let value = "b".repeat(INIT_VAL_SIZE);
                                hash_table_clone.put(key, value);
                            } else if prob < 0.90 {
                                let key_idx = rand::thread_rng().gen_range(0..NUM_KEYS);
                                let key = format!("{:08}", key_idx);
                                hash_table_clone.delete(&key);
                            } else {
                                let key_idx = rand::thread_rng().gen_range(0..NUM_KEYS);
                                let key = format!("{:08}", key_idx);
                                hash_table_clone.get(&key);
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_read_mostly, bench_write_mostly);
criterion_main!(benches);