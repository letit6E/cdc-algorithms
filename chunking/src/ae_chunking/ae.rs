use super::super::chunker::Chunker;
use std::f64::consts;

pub struct ChunkerAE {
    window_size: usize,
}

impl ChunkerAE {
    pub fn new(avg_size: usize) -> ChunkerAE {
        ChunkerAE {
            window_size: ((avg_size as f64) / (consts::E - 1.)).round() as usize,
        }
    }
}

impl Chunker for ChunkerAE {
    fn next_chunk(&mut self, data: &Vec<u8>, start: usize) -> usize {
        let mut max_value = data[start];
        let mut max_position = start;

        for i in start + 1..data.len() {
            if data[i] > max_value {
                max_value = data[i];
                max_position = i;
            } else {
                if i == max_position + self.window_size {
                    return i + 1;
                }
            }
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
