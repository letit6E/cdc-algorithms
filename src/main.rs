mod chunking;
use chunking::{ae::*, chunker::*, fastcdc::*, rabin::*};
use fastcdc::v2020::Chunk;
use rayon::prelude::{*, IndexedParallelIterator};

use std::cmp::max;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Cursor, Error, Read, Seek, SeekFrom};
use std::thread;
use std::time::{Duration, Instant};
use priority_queue::PriorityQueue;

fn main() {
    let path = "./resources/valvesockets.tar";
    let n = 16usize;
    let threads_cnt = 4usize;
    
    let chunker = AeChunker::new(1024 * n);
    let (x, y, z) = chunker.parallel_chunking(path, threads_cnt);
    println!("TIME = {:.2?}, COEFF = {}, AVG = {}", x, y, z);
}
