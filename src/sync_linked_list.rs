use std::{sync::RwLock, fmt::Debug};

pub struct SyncLinkedList<T> {
    max_size: usize,
    capacity: RwLock<usize>,
    data: RwLock<Vec<T>>,
    next: RwLock<Option<Box<SyncLinkedList<T>>>>,
}

impl<T> SyncLinkedList<T> where 
    T: Clone + PartialEq
{
    /* 
     * The new function
     */
    pub fn new(max_size: usize) -> Self {
        if max_size == 0 {
            panic!("Size of linked list should be non-zero");
        }

        SyncLinkedList {
            max_size,
            capacity: RwLock::new(max_size),
            data: RwLock::new(vec![]),
            next: RwLock::new(None),
        }
    }

    pub fn push(&self, item: T) {
        {
            let mut my_data = self.data.write().unwrap();
            for i in 0..my_data.len() {
                if my_data.get(i).unwrap().eq(&item) {
                    let _ = std::mem::replace(&mut my_data[i], item.clone());
                    return;
                }
            }

            if my_data.len() < self.capacity.read().unwrap().clone() {
                my_data.push(item.clone());
                return;
            }
        }
        let next_ref;
        let mut next;

        {
            next = self.next.write().unwrap();
            if next.is_none() {
                *next = Some(Box::new(SyncLinkedList::new(self.max_size.clone())));
            }
            next_ref = next.as_ref();
        }

        next_ref.unwrap().push(item.clone());
    }

    pub fn get(&self, item: &T) -> Option<T> {
        {
            let my_data = self.data.read().unwrap();
            let data_vec: &Vec<T> = my_data.as_ref();
            for element in data_vec {
                if element.eq(item) {
                    return Some(element.clone());
                }
            }
        }
        
        let mut next;

        let next_ref = {
            next = self.next.read().unwrap();
            if next.is_none() {
                return None;
            }
            next.as_ref()
        };

        next_ref.unwrap().get(item)
    }

    pub fn remove(&self, item: &T) {
        {
            let mut my_data = self.data.write().unwrap();
            for i in 0..my_data.len() {
                if my_data.get(i).unwrap().eq(&item) {
                    my_data.remove(i);
                    let mut capacity = self.capacity.write().unwrap();
                    *capacity -= 1;
                    return;
                }
            }
        }
        
        let mut next;

        let next_ref = {
            next = self.next.write().unwrap();
            if next.is_none() {
                return;
            }
            next.as_ref()
        };

        next_ref.unwrap().remove(item);
    }

    pub fn get_all_as_vec(&self) -> Vec<T> where T:Debug 
    {
        let mut result: Vec<T> = vec![];
        
        let my_data = self.data.read().unwrap().to_vec();
        
        result.extend(my_data.clone());

        // println!("Gathering {:?}", my_data.clone());

        if let Some(next) = self.next.read().unwrap().as_ref() {
            result.extend(next.get_all_as_vec());
        }

        result
    }
}

impl<T> Clone for SyncLinkedList<T> 
    where T: Clone
{
    fn clone(&self) -> Self {
        let data = self.data.read().unwrap().to_vec();

        Self { 
            max_size: self.max_size.clone(),
            capacity: RwLock::new(self.capacity.read().unwrap().clone()),
            data: RwLock::new(data),
            next: match self.next.read().unwrap().as_ref() {
                Some(n) => RwLock::new(Some(n.clone())),
                None => RwLock::new(None)
            }
        }
    }
}