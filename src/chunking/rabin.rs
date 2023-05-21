use super::chunker::{Chunker, ChunkerError, ChunkerStatus};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    ptr,
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

pub struct ChunkerRabin {
    buffer: [u8; 4096], // for buffered reading from input
    buffered: usize,    // count of first in buffer already taken bytes
    window_size: usize, // rabin fingerprint window size
    min_size: usize,    // min size of chunk
    max_size: usize,    // max size of chunk
    hash: HashRabin,    // struct for data of used Rabin hash parameters
}

impl ChunkerRabin {
    pub fn new(avg_size: usize, seed: u32) -> ChunkerRabin {
        let window_size = avg_size / 4 - 1;
        let min_size = avg_size / 4;
        let max_size = avg_size * 4;
        let alpha = 1_664_525;

        ChunkerRabin {
            buffer: [0; 4096],
            buffered: 0,
            window_size,
            min_size,
            max_size,
            hash: HashRabin::new(avg_size, window_size, alpha, seed),
        }
    }
}

impl Chunker for ChunkerRabin {
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
