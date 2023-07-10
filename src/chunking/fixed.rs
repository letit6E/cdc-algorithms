use super::chunker::*;
use std::cmp::min;
use std::io::{Read, Write};

pub struct FixedChunker {
    buffer: [u8; 4096],
    chunk_size: usize,
}

impl FixedChunker {
    pub fn new(expected_size: usize) -> FixedChunker {
        FixedChunker {
            buffer: [0; 4096],
            chunk_size: expected_size,
        }
    }
}

impl Chunker for FixedChunker {
    fn next_chunk(
        &mut self,
        input: &mut dyn Read,
        output: &mut dyn Write,
    ) -> Result<ChunkerStatus, ChunkerError> {
        // remaind to read
        let mut remainder = self.chunk_size;
        loop {
            // if chunk_size of remainder > chunk_size of buffer then at first read buffer chunk_size data
            let to_read = min(remainder, self.buffer.len());
            let read = input
                .read(&mut self.buffer[..to_read])
                .map_err(ChunkerError::Read)?;

            // end of input reached
            if read == 0 {
                return Ok(ChunkerStatus::Finished);
            }

            // write processed data
            output
                .write_all(&self.buffer[..read])
                .map_err(ChunkerError::Write)?;

            if read > remainder {
                return Ok(ChunkerStatus::Finished);
            }
            remainder -= read;
            if remainder == 0 {
                return Ok(ChunkerStatus::Working);
            }
        }
    }
}
