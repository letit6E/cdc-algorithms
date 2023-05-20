use super::chunker::Chunker;
use std::collections::VecDeque;

pub struct ChunkerRabin {
    window_size: usize,
    size_min: usize,
    size_max: usize,
    pow_table: [u32; 256],
    mask: u32,
    seed: u32,
    alpha: u32,
}

impl ChunkerRabin {
    pub fn new(avg_size: usize, seed: u32, alpha: u32) -> ChunkerRabin {
        let window_size = avg_size / 4 - 1;
        let size_min = avg_size / 4;
        let size_max = avg_size * 4;
        let mask = (avg_size as u32).next_power_of_two() - 1;

        let mut pow_table = [0u32; 256];
        let a = alpha.wrapping_pow(window_size as u32);
        for i in 0..pow_table.len() as u32 {
            pow_table[i as usize] = i.wrapping_mul(a);
        }

        ChunkerRabin {
            window_size,
            size_min,
            size_max,
            pow_table: [0u32; 256],
            mask,
            seed,
            alpha,
        }
    }
}

impl Chunker for ChunkerRabin {
    fn next_chunk(&mut self, data: &Vec<u8>, start: usize) -> usize {
        let mut hash = 0u32;
        let mut window: VecDeque<u8> = VecDeque::with_capacity(self.window_size);

        for i in start..data.len() {
            let value = data[i];

            if i - start >= self.size_max {
                return i + 1;
            }

            hash = hash.wrapping_mul(self.alpha).wrapping_add(value as u32);
            if i - start >= self.window_size {
                let first = window.pop_front().unwrap();
                hash = hash.wrapping_sub(self.pow_table[first as usize]);

                if i >= self.size_min && ((hash ^ self.seed) & self.mask) == 0 {
                    return i + 1;
                }
            }

            window.push_back(value);
        }

        data.len()
    }

    fn chunk(&mut self, data: &Vec<u8>) -> Vec<usize> {
        let mut result: Vec<usize> = vec![];

        let mut next = 0;
        while next < data.len() {
            next = self.next_chunk(data, next);
            result.push(next);
        }

        result
    }
}
