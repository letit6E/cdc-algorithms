const w: usize = 15;

fn chunking(data: Vec<u8>, start: usize) -> usize {
    let mut max_value = data[start];
    let mut max_position = start;

    for i in start + 1.. {
        if i >= data.len() {
            break;
        }

        if data[i] > max_value {
            max_value = data[i];
            max_position = i;
        } else {
            if i == max_position + w {
                return i;
            }
        }
    }
    return data.len();
}
