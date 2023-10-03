use std::{sync::RwLock, fmt::Debug};

pub struct SyncLinkedList<T> {
    max_size: usize,
    data: RwLock<Vec<T>>,
    next: RwLock<Option<Box<SyncLinkedList<T>>>>
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
            data: RwLock::new(vec![]),
            next: RwLock::new(None),
        }
    }

    // fn create_empty_next(&self) {
    //     let mut next = self.next.write().unwrap();
    //     *next = Some(Box::new(SyncLinkedList::new(self.max_size.clone())));
    // }

    pub fn push(&self, item: T) {
        {
            let mut my_data = self.data.write().unwrap();
            if my_data.len() < self.max_size {
                my_data.push(item.clone());
                return;
            }
        }
        
        let mut next;

        let next_ref = {
            next = self.next.write().unwrap();
            if next.is_none() {
                *next = Some(Box::new(SyncLinkedList::new(self.max_size.clone())));
            }
            next.as_ref()
        };

        next_ref.unwrap().push(item.clone());
    }

    pub fn find_item(&self, item: &T) -> Option<T> {
        {
            let my_data = self.data.read().unwrap();
            for data in my_data.to_vec() {
                if item.eq(&data) {
                    return Some(data)
                }
            }
        }

        if let Some(next) = self.next.read().unwrap().as_ref() {
            return next.find_item(item)
        }

        return None;
    }

    pub fn get_all_as_vec(&self) -> Vec<T> where T:Debug 
    {
        let mut result: Vec<T> = vec![];
        
        let my_data = self.data.read().unwrap().to_vec();
        
        result.extend(my_data.clone());

        println!("Gathering {:?}", my_data.clone());

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
            data: RwLock::new(data),
            next: match self.next.read().unwrap().as_ref() {
                Some(n) => RwLock::new(Some(n.clone())),
                None => RwLock::new(None)
            }
        }
    }
}