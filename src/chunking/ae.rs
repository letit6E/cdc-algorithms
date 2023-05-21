use super::chunker::*;
use std::f64::consts;
use std::io::{Read, Write};
use std::ptr;

// "AE: An Asymmetric Extremum Content Defined Chunking Algorithm for Fast and Bandwidth-Efficient Data Deduplication"
pub struct AeChunker {
    buffer: [u8; 4096], // for buffered reading from input(4096 is max size of chunk)
    buffered: usize,    // count of first already taken bytes
    window_size: usize, // size for extremum window
}

impl AeChunker {
    pub fn new(avg_size: usize) -> AeChunker {
        AeChunker {
            buffer: [0; 4096],
            buffered: 0,
            window_size: ((avg_size as f64) / (consts::E - 1.)).round() as usize, // from paper
        }
    }
}

impl Chunker for AeChunker {
    fn next_chunk(
        &mut self,
        input: &mut dyn Read,
        output: &mut dyn Write,
    ) -> Result<ChunkerStatus, ChunkerError> {
        let mut max_pos: usize = 0;
        let mut max_val: u8 = 0;
        loop {
            // [0, self.buffered - 1] is already taken bytes
            // max is end of data if it less than buffer size
            let max = input
                .read(&mut self.buffer[self.buffered..])
                .map_err(ChunkerError::Read)?
                + self.buffered;

            // if end of data is equal to 0 then there is no data
            if max == 0 {
                return Ok(ChunkerStatus::Finished);
            }

            for chunk_pos in 0..max {
                let cur_val = self.buffer[chunk_pos];

                if cur_val > max_val {
                    max_val = cur_val;
                    max_pos = chunk_pos;
                } else if chunk_pos == max_pos + self.window_size {
                    // returning chunk to output stream
                    output
                        .write_all(&self.buffer[chunk_pos + 1..])
                        .map_err(ChunkerError::Write)?;
                    // moving data to beginning of buffer(size of this data is equal to max-chunk_pos-1)
                    unsafe {
                        ptr::copy(
                            self.buffer[chunk_pos + 1..].as_ptr(),
                            self.buffer.as_mut_ptr(),
                            max - chunk_pos - 1,
                        )
                    };
                    self.buffered = max - chunk_pos - 1;

                    return Ok(ChunkerStatus::Working);
                }
            }

            // max size of chunk or end of input reached
            output
                .write_all(&self.buffer[..max])
                .map_err(ChunkerError::Write)?;
            self.buffered = 0;
        }
    }
}
