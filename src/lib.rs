use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use std::usize;


mod pair;
use pair::Pair;

mod sync_linked_list;
use sync_linked_list::{SyncLinkedList};

pub struct KVS {
    // TODO: define your key value store here...
    // NOTE: both key and value should be of type "String"!
    buckets: RwLock<Vec<RwLock<Vec<Pair<String, String>>>>>,
    size: RwLock<usize>,
    num_bins: usize,
    lock_granularity: usize
}

impl KVS
{
    const NUM_BINS: usize = 1000;
    const LOCK_GRANULARITY: usize = 100;

    fn _new(num_bins: usize, lock_granularity: usize) -> Self {
        let mut inner_vec = vec![];
        for _ in 0..num_bins {
            inner_vec.push(RwLock::new(vec![]));
        }

        KVS {
            // TODO: initialize your key value store here...
            buckets: RwLock::new(inner_vec),
            size: RwLock::new(0),
            num_bins,
            lock_granularity
        }
    }

    pub fn new() -> Self {
        KVS::_new(KVS::NUM_BINS, KVS::LOCK_GRANULARITY)
    }

    pub fn put(&self, key: String, value: String) {
        // TODO: complete the put function here...
        // Finding the hash
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let bin_index = (hasher.finish() % KVS::NUM_BINS as u64) as usize;
        
        let mut need_resize: bool = false;
        // Add the pair
        {
            let locked = self.buckets.read().unwrap();
            let bucket = locked.get(bin_index).unwrap();

            let mut inner_vec = bucket.write().unwrap();

            match inner_vec.binary_search_by_key(&key, |pair| pair.get_key().to_string()) {
                Ok(n) => {
                    inner_vec[n].update_value(value);
                },
                Err(index) => {
                    inner_vec.insert(index, Pair::new(key, value));
                    need_resize = inner_vec.len() > self.lock_granularity;
                }
            }
        }

        if need_resize {
            self.resize();
        }

    }

    fn resize(&self) {
        // Not implemented yet.
        // Should resize the hashmap. Right now we use a constant size for our hashtable.
    }

    pub fn get(&self, key: &str) -> Option<String> {
        // TODO: complete the get function here...
        // If the key exists, return the value wrapped in an Option, otherwise return None.

        // Finding the hash
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let bin_index = (hasher.finish() % KVS::NUM_BINS as u64) as usize;

        let locked = self.buckets.read().unwrap();
        let bucket = locked.get(bin_index).unwrap();

        let inner_vec = bucket.read().unwrap();

        match inner_vec.binary_search_by_key(&key, |pair| pair.get_key()) {
            Ok(n) => {
                return Some(inner_vec[n].get_value().clone());
            },
            Err(_) => {
                return None;
            }
        }
        

        // let pair = bucket.get(&Pair::new(key.to_string(), "".to_string()));
        // match pair {
        //     Some(p) => Some(p.get_value().clone()),
        //     None => None
        // }
    }

    pub fn delete(&self, key: &str) {
        // TODO: complete the delete function here...

        // Finding the hash
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let bin_index = (hasher.finish() % KVS::NUM_BINS as u64) as usize;

        // Add the pair
        let mut locked = self.buckets.write().unwrap();
        let bucket = locked.get_mut(bin_index).unwrap();

        let mut inner_vec = bucket.write().unwrap();
        
        match inner_vec.binary_search_by_key(&key, |pair| pair.get_key()) {
            Ok(n) => {
                inner_vec.remove(n);
            },
            Err(index) => {}
        }

        // bucket.remove(&Pair::new(key.to_string(), "".to_string()));
    }

    pub fn inner_table(&self) -> HashMap<String, String> {
        // TODO: complete the inner_table function here...
        // This will be only used for testing and debugging purposes.
        // It should convert and return the internal data as a standard hash map.
        let mut out = HashMap::new();

        for i in 0..KVS::NUM_BINS {
            let locked = self.buckets.read().unwrap();
            let bucket = locked.get(i).unwrap();

            let inner_vec = bucket.read().unwrap();

            for pair in inner_vec.to_vec() {
                out.insert(pair.get_key().clone(), pair.get_value().clone());
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_put_and_get() {
        let hash_table = KVS::new();
        hash_table.put("key1".to_string(), "value1".to_string());
        hash_table.put("key2".to_string(), "value2".to_string());

        assert_eq!(hash_table.get(&"key1"), Some("value1".to_string()));
        assert_eq!(hash_table.get(&"key2"), Some("value2".to_string()));
        assert_eq!(hash_table.get(&"key3"), None);
    }

    #[test]
    fn test_delete() {
        let hash_table = KVS::new();
        hash_table.put("key1".to_string(), "value1".to_string());
        hash_table.put("key2".to_string(), "value2".to_string());

        hash_table.delete(&"key1");
        assert_eq!(hash_table.get(&"key1"), None);
        assert_eq!(hash_table.get(&"key2"), Some("value2".to_string()));
    }
}