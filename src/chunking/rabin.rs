use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use super::chunker::{Chunker, ChunkerError, ChunkerStatus};
use std::{
    collections::{VecDeque, HashSet, hash_map::DefaultHasher},
    io::{Read, Write},
    ptr, cmp::max, time::{Instant, Duration}, hash::{Hash, Hasher}, thread,
};

// struct for storing data of Rabin hash
struct HashRabin {
    pow_table: [u32; 256], // alpha ^ (hash_exp * chunk_pos) mod 2^32 for chunk_pos in 0..257
    mask: u32,
    seed: u32,
    alpha: u32,
}

impl HashRabin {
    pub fn new(mask_key: usize, hash_exp: usize, alpha: u32, seed: u32) -> HashRabin {
        let mut pow_table = [0u32; 256];
        let a = alpha.wrapping_pow(hash_exp as u32);
        for chunk_pos in 0..pow_table.len() as u32 {
            pow_table[chunk_pos as usize] = chunk_pos.wrapping_mul(a);
        }

        HashRabin {
            pow_table,
            mask: (mask_key as u32).next_power_of_two() - 1,
            seed,
            alpha,
        }
    }
}

pub struct RabinChunker {
    buffer: [u8; 4096], // for buffered reading from input
    buffered: usize,    // count of first in buffer already taken bytes
    window_size: usize, // rabin fingerprint window size
    min_size: usize,    // min size of chunk
    max_size: usize,    // max size of chunk
    hash: HashRabin,    // struct for data of used Rabin hash parameters
}

impl RabinChunker {
    pub fn new(expected_size: usize, seed: u32) -> RabinChunker {
        let window_size = expected_size / 4 - 1;
        let min_size = expected_size / 4;
        let max_size = expected_size * 4;
        let alpha = 1_664_525;

        RabinChunker {
            buffer: [0; 4096],
            buffered: 0,
            window_size,
            min_size,
            max_size,
            hash: HashRabin::new(expected_size, window_size, alpha, seed),
        }
    }

    pub fn get_bounds(&self, vec: &Vec<u8>, left: usize, right: usize) -> Vec<usize> {
        let mut result = Vec::new();
        let mut hash = 0u32;
        let start = max(0i64, (left as i64) - (self.window_size as i64)) as usize;
        let mut last_pos = start;
        let mut window = VecDeque::with_capacity(self.window_size);
        for i in start..right {
            hash = hash
                .wrapping_mul(self.hash.alpha)
                .wrapping_add(vec[i] as u32);
            if i >= last_pos + self.window_size {
                let front_window = window.pop_front().unwrap();
                hash = hash.wrapping_sub(self.hash.pow_table[front_window as usize]);

                // next chunk finded
                if i >= last_pos + self.min_size && ((hash ^ self.hash.seed) & self.hash.mask) == 0
                {
                    result.push(i);
                    window = VecDeque::with_capacity(self.window_size);
                    last_pos = i - 1;
                    hash = 0u32;
                    continue;
                }
            }

            window.push_back(vec[i]);
        }

        result
    }

    pub fn parallel_chunking(&self, path: &str, threads_cnt: usize) -> (Duration, f64, f64) {
        let vec = Chunker::read_file(self, path).unwrap();
        let sz = vec.len();

        let mut threads = vec![];
        let now = Instant::now();
        for i in 1..threads_cnt + 1 {
            let tmp = vec.clone();
            let self_clone = RabinChunker::new(self.max_size / 4, self.hash.seed);
            threads.push(thread::spawn(move || self_clone.get_bounds(&tmp,  (i - 1) * tmp.len() / threads_cnt, tmp.len() / threads_cnt)));
        }

        let mut last: i64 = -1;
        let mut set = HashSet::new();
        let mut local_sm = 0;
        let mut sm = 0;
        for handle in threads {
            for elem in handle.join().unwrap() {
                if (elem as i64) - last >= (self.window_size as i64) {
                    let next = &vec[((last + 1) as usize)..elem + 1];
                    let mut hasher = DefaultHasher::new();
                    next.hash(&mut hasher);
                    let hsh = hasher.finish();
    
                    if !set.contains(&hsh) {
                        set.insert(hsh);
                        local_sm += (elem as i64) - last;
                    }
                    sm += (elem as i64) - last;
                    last = elem as i64;
                }
            }
        }
        
        let elapsed = now.elapsed();
        (elapsed, (local_sm as f64) / (sz as f64), (sm as f64) / (set.len() as f64))
    }
}

impl Chunker for RabinChunker {
    fn next_chunk(
        &mut self,
        input: &mut dyn Read,
        output: &mut dyn Write,
    ) -> Result<ChunkerStatus, ChunkerError> {
        let mut hash = 0u32;
        let mut local_pos = 0;
        let mut window = VecDeque::with_capacity(self.window_size);
        loop {
            // [0, self.buffered - 1] is already taken bytes
            // max is end of data if it less than buffer size
            let max = input
                .read(&mut self.buffer[self.buffered..])
                .map_err(ChunkerError::Read)?
                + self.buffered;

            // if max == 0 then end of input reached
            if max == 0 {
                return Ok(ChunkerStatus::Finished);
            }

            for i in 0..max {
                let cur_val = self.buffer[i];

                // max chunk size reached
                if local_pos >= self.max_size {
                    // write remained data
                    output
                        .write_all(&self.buffer[..i + 1])
                        .map_err(ChunkerError::Write)?;

                    // move unprocessed data to the beginning of buffer
                    unsafe {
                        ptr::copy(
                            self.buffer[i + 1..].as_ptr(),
                            self.buffer.as_mut_ptr(),
                            max - i - 1,
                        )
                    };
                    self.buffered = max - i - 1;

                    return Ok(ChunkerStatus::Working);
                }

                // Rabin fingerprint main algorithm
                hash = hash
                    .wrapping_mul(self.hash.alpha)
                    .wrapping_add(cur_val as u32);
                if local_pos >= self.window_size {
                    let front_window = window.pop_front().unwrap();
                    hash = hash.wrapping_sub(self.hash.pow_table[front_window as usize]);

                    // next chunk finded
                    if local_pos >= self.min_size && ((hash ^ self.hash.seed) & self.hash.mask) == 0
                    {
                        // write remained data
                        output
                            .write_all(&self.buffer[..i + 1])
                            .map_err(ChunkerError::Write)?;

                        // move unprocessed data to the beginning of buffer
                        unsafe {
                            ptr::copy(
                                self.buffer[i + 1..].as_ptr(),
                                self.buffer.as_mut_ptr(),
                                max - i - 1,
                            )
                        };
                        self.buffered = max - i - 1;

                        return Ok(ChunkerStatus::Working);
                    }
                }
                local_pos += 1;
                window.push_back(cur_val);
            }

            // end of input or end of buffer reached
            output
                .write_all(&self.buffer[..max])
                .map_err(ChunkerError::Write)?;
            self.buffered = 0;
        }
    }
}
