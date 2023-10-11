use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use super::chunker::*;
use std::cmp::max;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::f64::consts;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write, Error};
use std::ptr;
use std::time::{Instant, Duration};

// "AE: An Asymmetric Extremum Content Defined Chunking Algorithm for Fast and Bandwidth-Efficient Data Deduplication"
pub struct AeChunker {
    buffer: [u8; 4096], // for buffered reading from input(4096 is max size of chunk)
    buffered: usize,    // count of first in buffer already taken bytes
    window_size: usize, // size for extremum window
}

impl AeChunker {
    pub fn new(expected_size: usize) -> AeChunker {
        AeChunker {
            buffer: [0; 4096],
            buffered: 0,
            window_size: expected_size - 256,
            // window_size: ((expected_size as f64) / (consts::E - 1.)).round() as usize, // from paper
        }
    }

    fn get_bounds(&self, vec: &Vec<u8>, left: usize, right: usize) -> Vec<usize> {
        let mut result = Vec::new();
        let start = max(0i64, (left as i64) - (self.window_size as i64)) as usize;
        let mut max_val = vec[start];
        let mut max_pos = start;
        for i in start..right {
            if vec[i] > max_val {
                max_val = vec[i];
                max_pos = i;
            } else if i == max_pos + self.window_size {
                result.push(i);
                if i + 1 != right {
                    max_val = vec[i + 1];
                    max_pos = i + 1;
                }
            }
        }
    
        if right == vec.len() {
            result.push(vec.len() - 1);
        }
        result
    }
    
    pub fn parallel_chunking(&self, path: &str, threads_cnt: usize) -> (Duration, f64, f64) {
        let vec = Chunker::read_file(self, path).unwrap();
    
        let now = Instant::now();
        let tmp: Vec<Vec<usize>> = (1..threads_cnt + 1)
                .into_par_iter()
                .map(|i| {
                    let left = (i - 1) * vec.len() / threads_cnt;
                    let right = i * vec.len() / threads_cnt;
                    self.get_bounds(&vec,  left, right)
                })
                .collect();
    
        let mut last: i64 = -1;
        let mut set = HashSet::new();
        let mut local_sm = 0;
        let mut sm = 0;
        for j in 0..threads_cnt {
            for &elem in &tmp[j] {
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
        (elapsed, (local_sm as f64) / (vec.len() as f64), (sm as f64) / (set.len() as f64))
    }
}

impl Chunker for AeChunker {
    fn next_chunk(
        &mut self,
        input: &mut dyn Read,
        output: &mut dyn Write,
    ) -> Result<ChunkerStatus, ChunkerError> {
        let mut local_pos = 0;
        let mut max_pos = 0;
        let mut max_val = 0;
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
                if cur_val > max_val {
                    max_val = cur_val;
                    max_pos = local_pos;
                } else if local_pos == max_pos + self.window_size {
                    // finded new chunk, write it
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
                local_pos += 1;
            }

            // end of buffer or end of input reached
            output
                .write_all(&self.buffer[..max])
                .map_err(ChunkerError::Write)?;
            self.buffered = 0;
        }
    }

    
}
