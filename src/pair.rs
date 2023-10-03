#[derive(Debug)]
pub struct Pair<K, V> {
    key: K,
    value: V
}

impl<K, V> Pair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Pair {
            key,
            value
        }
    }

    pub fn get_key(&self) -> &K {
        return &self.key
    }

    pub fn get_value(&self) -> &V {
        return &self.value
    }
}

impl<K, V> Clone for Pair<K, V> where 
    K: Clone,
    V: Clone
{
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone()
        }
    }
}

impl<K, V> PartialEq for Pair<K, V> where
    K: PartialEq
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

