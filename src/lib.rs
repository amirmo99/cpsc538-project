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
    buckets: RwLock<Vec<SyncLinkedList<Pair<String, String>>>>
}

impl KVS
{
    const NUM_BINS: usize = 100;
    const LOCK_GRANULARITY: usize = 100;

    pub fn new() -> Self {
        KVS {
            // TODO: initialize your key value store here...
            buckets: RwLock::new(vec![SyncLinkedList::new(KVS::LOCK_GRANULARITY); KVS::NUM_BINS]) //
        }
    }

    pub fn put(&self, key: String, value: String) {
        // TODO: complete the put function here...
        // Finding the hash
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let bin_index = (hasher.finish() % KVS::NUM_BINS as u64) as usize;

        // Resize if needed
        
        // Add the pair
        let locked = self.buckets.read().unwrap();
        let bucket = locked.get(bin_index).unwrap();

        bucket.push(Pair::new(key, value));
        
    }

    fn resize_if_needed(&self, index: usize) {
        todo!()
    }

    pub fn get(&self, key: &str) -> Option<String> {
        // TODO: complete the get function here...
        // If the key exists, return the value wrapped in an Option, otherwise return None.
        todo!()
    }

    pub fn delete(&self, key: &str) {
        // TODO: complete the delete function here...
        todo!()
    }

    pub fn inner_table(&self) -> HashMap<String, String> {
        // TODO: complete the inner_table function here...
        // This will be only used for testing and debugging purposes.
        // It should convert and return the internal data as a standard hash map.
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list() {
        let list = SyncLinkedList::new(10);
        
        for i in 0..1000 {
            list.push(i);
        }

        let all = list.get_all_as_vec();
        println!("** All: {:?}, \n**len={}", all, all.len());
    }
    #[test]
    fn test_other_file() {
        let mut x = Pair::new(10, 20);

        let u = x.get_key();
        x.update_value(56);
    }

    #[test]
    fn test_put_and_get() {
        let mut hash_table = KVS::new();
        hash_table.put("key1".to_string(), "value1".to_string());
        hash_table.put("key2".to_string(), "value2".to_string());

        assert_eq!(hash_table.get(&"key1"), Some("value1".to_string()));
        assert_eq!(hash_table.get(&"key2"), Some("value2".to_string()));
        assert_eq!(hash_table.get(&"key3"), None);
    }

    #[test]
    fn test_delete() {
        let mut hash_table = KVS::new();
        hash_table.put("key1".to_string(), "value1".to_string());
        hash_table.put("key2".to_string(), "value2".to_string());

        hash_table.delete(&"key1");
        assert_eq!(hash_table.get(&"key1"), None);
        assert_eq!(hash_table.get(&"key2"), Some("value2".to_string()));
    }
}