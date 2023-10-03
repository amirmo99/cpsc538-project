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

        // Add the pair
        let locked = self.buckets.read().unwrap();
        let bucket = locked.get(bin_index).unwrap();

        bucket.push(Pair::new(key, value));
        
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

        let pair = bucket.get(&Pair::new(key.to_string(), "".to_string()));
        match pair {
            Some(p) => Some(p.get_value().clone()),
            None => None
        }
    }

    pub fn delete(&self, key: &str) {
        // TODO: complete the delete function here...

        // Finding the hash
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let bin_index = (hasher.finish() % KVS::NUM_BINS as u64) as usize;

        let locked = self.buckets.read().unwrap();
        let bucket = locked.get(bin_index).unwrap();

        bucket.remove(&Pair::new(key.to_string(), "".to_string()));
    }

    pub fn inner_table(&self) -> HashMap<String, String> {
        // TODO: complete the inner_table function here...
        // This will be only used for testing and debugging purposes.
        // It should convert and return the internal data as a standard hash map.
        let mut out = HashMap::new();

        for i in 0..KVS::NUM_BINS {
            let locked = self.buckets.read().unwrap();
            let bucket = locked.get(i).unwrap();

            let all = bucket.get_all_as_vec();
            for pair in all {
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
    fn test_linked_list() {
        let m = 20;

        // Test with Pair
        let list: SyncLinkedList<Pair<String, String>> = SyncLinkedList::new(10);
        for i in 0..m {
            list.push(Pair::new(i.to_string(), format!("value_{}", i)));
        }

        let all = list.get_all_as_vec();
        // println!("** All: {:?}, \n**len={}", all, all.len());
        assert_eq!(all.len(), m);

        list.push(Pair::new("ABC".to_string(), "000".to_string()));
        println!("Value before: {:?}", list.get(&Pair::new("ABC".to_string(), "".to_string())));

        list.push(Pair::new("ABC".to_string(), "777".to_string()));
        println!("Value after: {:?}", list.get(&Pair::new("ABC".to_string(), "".to_string())));

        // println!("** All: {:?}", list.get_all_as_vec());
        
        list.remove(&Pair::new("ABC".to_string(), "".to_string()));
        println!("Value removed: {:?}", list.get(&Pair::new("ABC".to_string(), "".to_string())));

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