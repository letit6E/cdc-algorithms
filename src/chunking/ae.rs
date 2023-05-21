use super::chunker::*;
use std::f64::consts;
use std::io::{Read, Write};
use std::ptr;

pub struct AeChunker {
    buffer: [u8; 0x1000],
    left: usize,
    window_size: usize,
}

impl AeChunker {
    pub fn new(avg_size: usize) -> AeChunker {
        AeChunker {
            buffer: [0; 0x1000],
            left: 0,
            window_size: ((avg_size as f64) / (consts::E - 1.)).round() as usize,
        }
    }
}

impl Chunker for AeChunker {
    fn next_chunk(
        &mut self,
        r: &mut dyn Read,
        w: &mut dyn Write,
    ) -> Result<ChunkerStatus, ChunkerError> {
        let mut global_pos: usize = 0;
        let mut max_pos: usize = 0;
        let mut max_val: u8 = 0;
        loop {
            let mut max = r
                .read(&mut self.buffer[self.left..])
                .map_err(ChunkerError::Read)?
                + self.left;

            if max == 0 {
                return Ok(ChunkerStatus::Finished);
            }

            for i in 0..max {
                let cur_val = self.buffer[i];

                if cur_val > max_val {
                    max_val = cur_val;
                    max_pos = global_pos;
                } else if global_pos == max_pos + self.window_size {
                    w.write_all(&self.buffer[i + 1..])
                        .map_err(ChunkerError::Write)?;
                    unsafe {
                        ptr::copy(
                            self.buffer[i + 1..].as_ptr(),
                            self.buffer.as_mut_ptr(),
                            max - i - 1,
                        )
                    };
                    self.left = max - i - 1;
                    return Ok(ChunkerStatus::Working);
                }

                global_pos += 1;
            }

            w.write_all(&self.buffer[..max])
                .map_err(ChunkerError::Write);
            self.left = 0;
        }
    }
}
