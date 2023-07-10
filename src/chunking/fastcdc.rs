use super::chunker::*;
use std::io::{Read, Write};
use std::ptr;

// "FastCDC: a Fast and Efficient Content-Defined Chunking Approach for Data Deduplication"

// https://stackoverflow.com/questions/66168970/find-more-indipendent-seed-value-for-a-64-bit-lcg-mmix-by-knuth
// random numbers for every byte value: 0..256
fn generate_seq(seed: u64) -> [u64; 256] {
    let mut result = [0u64; 256];
    let alpha = 6364136223846793005;
    let gamma = 1442695040888963407;

    let mut cur_value = seed;
    for element in &mut result.iter_mut() {
        cur_value = cur_value.wrapping_mul(alpha).wrapping_add(gamma);
        *element = cur_value;
    }

    result
}

fn generate_masks(expected_size: usize, noice: usize, seed: u64) -> (u64, u64) {
    let bits_count = (expected_size.next_power_of_two() - 1).count_ones();
    if bits_count == 13 {
        // From paper
        return (0x0003590703530000, 0x0000d90003530000);
    }

    let mut mask = 0u64;
    let mut cur_value = seed;
    let alpha = 6364136223846793005;
    let gamma = 1442695040888963407;

    while mask.count_ones() < bits_count - noice as u32 {
        cur_value = cur_value.wrapping_mul(alpha).wrapping_add(gamma);
        mask = (mask | 1).rotate_left(cur_value as u32 & 0x3f);
    }
    let long_mask = mask;

    while mask.count_ones() < bits_count + noice as u32 {
        cur_value = cur_value.wrapping_mul(alpha).wrapping_add(gamma);
        mask = (mask | 1).rotate_left(cur_value as u32 & 0x3f);
    }
    let short_mask = mask;

    (short_mask, long_mask)
}

pub struct FastCdcChunker {
    buffer: [u8; 4096],
    buffered: usize,
    gear: [u64; 256],
    min_size: usize,
    max_size: usize,
    expected_size: usize,
    long_mask: u64,
    short_mask: u64,
}

impl FastCdcChunker {
    pub fn new(expected_size: usize, seed: u64) -> Self {
        let (mask_short, mask_long) = generate_masks(expected_size, 1, seed);
        FastCdcChunker {
            buffer: [0; 4096],
            buffered: 0,
            gear: generate_seq(seed),
            min_size: expected_size / 4,
            max_size: expected_size * 8,
            expected_size: expected_size,
            long_mask: mask_long,
            short_mask: mask_short,
        }
    }
}

impl Chunker for FastCdcChunker {
    fn next_chunk(
        &mut self,
        input: &mut dyn Read,
        output: &mut dyn Write,
    ) -> Result<ChunkerStatus, ChunkerError> {
        let mut hash = 0u64;
        let mut local_pos = 0;
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
                if local_pos >= self.min_size {
                    hash = (hash << 1).wrapping_add(self.gear[self.buffer[i] as usize]);

                    // 3 cases for new chunk:
                    if local_pos < self.expected_size && (hash & self.short_mask == 0)
                        || local_pos >= self.expected_size && (hash & self.long_mask == 0)
                        || local_pos >= self.max_size
                    {
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
