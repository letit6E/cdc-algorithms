use super::chunker::*;
use std::f64::consts;
use std::io::{Read, Write};
use std::ptr;

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
