use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use std::sync::{Arc,atomic::{AtomicUsize}};

pub struct Producer<const N: usize, T> {
    buffer: Arc<SpscRingBuffer<N, T>>,
    local_head: usize,
    cached_tail: usize
}

pub struct Consumer<const N: usize, T> {
    buffer: Arc<SpscRingBuffer<N, T>>,
    cached_head: usize,
    local_tail: usize
}

impl<const N: usize, T> Producer<N, T> {
    pub async fn push(&mut self, value: T) -> Result<(), T> {
        if self.local_head - self.cached_tail >= N {
            self.cached_tail = self.buffer.tail.0.load(Ordering::Acquire);

            if self.local_head - self.cached_tail >= N {
                return Err(value);
            }
        }

        
        let index = self.local_head & (N - 1);
            unsafe {
               (*self.buffer.buffer[index].get()).write(value);
            }
            self.local_head += 1;
            self.buffer.head.0.store(self.local_head + 1, Ordering::Release);
            Ok(())
    }
}

impl<const N: usize, T> Consumer<N, T> {
    pub async fn pop(&mut self) -> Option<T> {
        if self.cached_head == self.local_tail {
            self.cached_head = self.buffer.head.0.load(Ordering::Acquire);
    
           if self.cached_head == self.local_tail {
                return None;
           }
       }

       let index = self.local_tail & (N - 1);
       
       let value = unsafe {
           (*self.buffer.buffer[index].get()).assume_init_read()
       };
       self.local_tail += 1;
       self.buffer.tail.0.store(self.local_tail, Ordering::Release);
       Some(value)
    }
}
#[repr(align(64))]
struct CachePadded(AtomicUsize);

pub struct SpscRingBuffer<const N: usize, T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>; N]>,
    head: CachePadded,
    tail: CachePadded
}

unsafe impl<const N: usize, T> Sync for SpscRingBuffer<N,T> {}

impl<const N: usize, T> SpscRingBuffer<N, T> {
    pub async fn new() -> (Producer<N, T>, Consumer<N, T>) {
        assert!(N.is_power_of_two(),"Буффер должен быть степень двойки!!!");

        let buffer = Arc::new(Self {
            buffer: Box::new(std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::<T>::uninit()))),
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0))
        });

        let producer = Producer {
            buffer: Arc::clone(&buffer),
            local_head: 0,
            cached_tail: 0
        };

        let consumer = Consumer {
            buffer: Arc::clone(&buffer),
            local_tail: 0,
            cached_head: 0
        };

        (producer, consumer)
    }
}