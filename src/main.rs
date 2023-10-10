mod chunking;
use chunking::{ae::*, chunker::*, fastcdc::*, rabin::*};
use fastcdc::v2020::Chunk;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Cursor, Error, Read, Seek, SeekFrom};
use std::thread;
use std::time::Instant;

fn test_chunking(chunker: &mut dyn Chunker, data: &mut dyn Read) -> Vec<Vec<u8>> {
    let mut chunks: Vec<Vec<u8>> = vec![];
    let mut chunk: Vec<u8> = vec![];
    while chunker.next_chunk(data, &mut chunk).unwrap() == ChunkerStatus::Working {
        chunks.push(chunk);
        chunk = vec![];
    }
    chunks.push(chunk);

    chunks
}

fn internal_chunking(path: &str, n: usize) -> Vec<Chunk> {
    let contents = std::fs::read(path).unwrap();
    let chunker = fastcdc::v2020::FastCDC::new(
        &contents,
        (n as u32) * 256,
        (n as u32) * 1024,
        (n as u32) * 8 * 1024,
    );
    let mut chunks: Vec<fastcdc::v2020::Chunk> = vec![];

    for chunk in chunker {
        chunks.push(chunk);
    }
    chunks
}

fn fcdctest(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = FastCdcChunker::new(1024 * n, 1);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn aetest(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = AeChunker::new(1024 * n);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn rabintest(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = RabinChunker::new(1024 * n, 0);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn read_file(path: &str) -> Result<Vec<u8>, Error> {
    let mut f = File::open(path)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;

    Ok(buffer)
}

fn hash_vec<T: Hash>(v: &Vec<T>) -> u64 {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    hasher.finish()
}
fn main() {
    let path = "./resources/test.mkv";
    let n = 64usize;
    let threads_cnt = 4usize;

    // let now = Instant::now();
    // let result = internal_chunking(path, n);
    // let mut set = HashSet::new();
    // let mut sm_data = 0;
    // let mut sm_len = 0;
    // for elem in &result {
    //     sm_len += elem.length;
    //     let hsh = elem.hash;
    //     if !set.contains(&hsh) {
    //         set.insert(hsh);
    //         sm_data += elem.length;
    //     }
    // }
    let now = Instant::now();
    let result = aetest(path, n);
    let mut set = HashSet::new();
    let mut sm_data = 0;
    let mut sm_len = 0;
    for elem in &result {
        sm_len += elem.len();
        let hsh = hash_vec(&elem);
        if !set.contains(&hsh) {
            set.insert(hsh);
            sm_data += elem.len();
        }
    }
    let elapsed = now.elapsed();
    
    println!("=========\nTIME  = {:.3?}\n==========", elapsed);
    println!("=========\nCOEFF = {:.3?}\n==========", (sm_data as f64) / (sm_len as f64));
    println!("=========\nAVG   = {:.3?}\n==========", (sm_len as f64) / (result.len() as f64));
    println!("=========\nFILE  = {:.3?}\n==========", sm_len);

    return;

    let buffer = read_file(path).unwrap();
    let mut vecs: Vec<Vec<u8>> = vec![Vec::new(); threads_cnt];
    for i in 0..threads_cnt {
        let left = i * buffer.len() / threads_cnt;
        let right = (i + 1) * buffer.len() / threads_cnt;
        vecs[i] = (buffer[left..right]).to_vec();
    }

    let now = Instant::now();
    let (len, cnt) = {
        let mut threads = vec![];

        for i in 0..threads_cnt {
            let cur = vecs[i].clone();
            threads.push(thread::spawn(move || {
                let mut chunker = RabinChunker::new(1024 * n, 0);
                let mut cursor = Cursor::new(cur);
                let mut set = HashSet::new();
                let mut result = 0;

                let vecs = test_chunking(&mut chunker, &mut cursor);
                let left = if i % 2 == 0 { 0 } else { 1 };
                let right = if i % 2 == 0 {
                    vecs.len() - 1
                } else {
                    vecs.len()
                };

                for i in left..right {
                    let hsh = hash_vec(&vecs[i]);
                    if !set.contains(&hsh) {
                        set.insert(hsh);
                        result += vecs[i].len();
                    }
                }

                let remainder = if i % 2 == 0 {
                    vecs.last()
                } else {
                    vecs.first()
                };
                (set, result, remainder.unwrap().clone())
            }));
        }

        let mut result_set: HashSet<u64> = HashSet::new();
        let mut ans = 0;
        let mut remainders = vec![];
        for handle in threads {
            let (st, x, y) = handle.join().unwrap();
            result_set.extend(&st);
            remainders.push(y);
            ans += x;
        }

        let mut i = 0;
        while i < remainders.len() - 1 {
            let mut t = remainders[i + 1].clone();
            remainders[i].append(&mut t);
            let mut chunker = RabinChunker::new(1024 * n, 0);
            let mut cursor = Cursor::new(&remainders[i]);
            for elem in test_chunking(&mut chunker, &mut cursor) {
                let hsh = hash_vec(&elem);
                if !result_set.contains(&hsh) {
                    result_set.insert(hsh);
                    ans += elem.len();
                }
            }
            i += 2;
        }

        (ans, result_set.len())
    };
    let elapsed = now.elapsed();
    println!(
        "=========\nTIME = {:.3?}, COEFF = {}, AVG SIZE = {} \n==========",
        elapsed,
        (len as f64) / (buffer.len() as f64),
        (len as f64) / (cnt as f64)
    );
    // let internal_result = internal_chunking(n, path);
    // let fcdc_result = fcdctest(path, n);
    // let ae_result = aetest(path, n);
    // let rabin_result = rabintest(path, n);
    // println!(
    //     "{} {} {} {}",
    //     internal_result.len(),
    //     fcdc_result.len(),
    //     ae_result.len(),
    //     rabin_result.len()
    // );
}
