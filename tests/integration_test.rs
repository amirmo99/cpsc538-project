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
#[test]
fn test_integration_put_duplicate_keys() {
    const NUM_THREADS: usize = 8;
    const NUM_KEYS: usize = 50000;

    let hash_table = Arc::new(KVS::new());
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles_put = (0..NUM_THREADS)
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

    for handle in handles_put {
        handle.join().unwrap();
    }

    let handles_replace = (0..NUM_THREADS)
        .map(|i| {
            let hash_table_clone = Arc::clone(&hash_table);
            let barrier_clone = barrier.clone();

            thread::spawn(move || {
                barrier_clone.wait();

                for j in 0..NUM_KEYS {
                    let key = format!("key_{}_{}", i, j);
                    let value = format!("value_{}_{}_new", i, j);

                    hash_table_clone.put(key.clone(), value.clone());
                    let retrieved_value = hash_table_clone.get(&key);
                    assert_eq!(retrieved_value, Some(value));
                    
                    hash_table_clone.delete(&key);
                    let retrieved_value = hash_table_clone.get(&key);
                    assert_eq!(retrieved_value, None);
                }
                
            })
        })
        .collect::<Vec<_>>();

    for handle in handles_replace {
        handle.join().unwrap();
    }
}

#[test]
fn test_integration_delete() {
    const NUM_THREADS: usize = 8;
    const NUM_KEYS: usize = 10000;

    let hash_table = Arc::new(KVS::new());
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles_put = (0..NUM_THREADS)
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

    for handle in handles_put {
        handle.join().unwrap();
    }

    let handles_delete = (0..NUM_THREADS)
        .map(|i| {
            let hash_table_clone = Arc::clone(&hash_table);
            let barrier_clone = barrier.clone();

            thread::spawn(move || {
                barrier_clone.wait();

                for j in 0..NUM_KEYS {
                    let key = format!("key_{}_{}", i, j);
                    hash_table_clone.delete(&key);
                    
                    let retrieved_value = hash_table_clone.get(&key);
                    assert_eq!(retrieved_value, None);
                }
                
            })
        })
        .collect::<Vec<_>>();

    for handle in handles_delete {
        handle.join().unwrap();
    }
}