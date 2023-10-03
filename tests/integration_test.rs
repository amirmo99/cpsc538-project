use kv_store::{KVS};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::collections::HashMap;

#[test]
fn test_integration() {
    const NUM_THREADS: usize = 8;
    const NUM_KEYS: usize = 100000;

    let hash_table = Arc::new(KVS::new());
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles = (0..NUM_THREADS)
        .map(|i| {
            let hash_table_clone = Arc::clone(&hash_table);
            let barrier_clone = barrier.clone();

            thread::spawn(move || {
                barrier_clone.wait();

                for j in 0..NUM_KEYS {
                    let key = format!("key_{}_{}", i, j);
                    let value = format!("value_{}_{}", i, j);
                    hash_table_clone.put(key.clone(), value.clone());

                    let retrieved_value = hash_table_clone.get(&key);
                    assert_eq!(retrieved_value, Some(value));
                }
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap();
    }
}

// TODO: write 2 more integration tests here...