use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::path::Path;
use std::str;
use std::str::FromStr;
use std::string::ParseError;

use itertools::Itertools;
use rayon::prelude::*;

const EOL: u8 = 0x0A;

#[derive(Debug)]
struct Result {
    min: f64,
    max: f64,
    mean: f64,
    len: u32,
}

#[derive(Debug, Clone, Copy)]
struct ResultHackedFloat {
    min: HackFloat,
    max: HackFloat,
    mean: f64,
    len: u32,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
struct HackFloat {
    digit: isize,
    decimal: usize,
}

impl Ord for HackFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.digit == other.digit {
            if self.decimal == other.decimal {
                return Ordering::Equal;
            } else if self.decimal < other.decimal {
                return Ordering::Less;
            } else {
                return Ordering::Greater;
            }
        } else if self.digit < other.digit {
            return Ordering::Less;
        } else {
            return Ordering::Greater;
        }
    }
}

impl FromStr for HackFloat {
    type Err = ParseError;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let (digit, decimal) = s.split_at(s.len() - 2);
        return Ok(HackFloat {
            digit: digit.parse::<isize>().unwrap(),
            decimal: decimal[1..].parse::<usize>().unwrap(),
        });
    }
}

fn main() {
    // single_threaded();
    //single_threaded_hacked_floats();
    //just_read_file();

    fast_read_lines("./measurements.txt");
    //just_read_file_rayon();
    //multi_process_rayon();
}

fn single_threaded_hacked_floats() {
    // File hosts.txt must exist in the current path
    let mut map = HashMap::<String, ResultHackedFloat>::with_capacity(10000);
    if let Ok(lines) = read_lines("./measurements.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines.flatten() {
            let (city, temp) = line.split(";").next_tuple().unwrap();
            let temp_hacked = temp.parse::<HackFloat>().unwrap();
            let temp_f64 = temp.parse::<f64>().unwrap();
            if let Some(c) = map.get_mut(city) {
                if temp_hacked < c.min {
                    c.min = temp_hacked;
                }
                if temp_hacked > c.max {
                    c.max = temp_hacked;
                }
                let new_len = c.len + 1;
                c.mean = c.mean + ((temp_f64 - c.mean) / new_len as f64);
                c.len = new_len;
            } else {
                map.insert(
                    city.to_string(),
                    ResultHackedFloat {
                        min: temp_hacked,
                        max: temp_hacked,
                        mean: temp_f64,
                        len: 1,
                    },
                );
            }
        }
    }
    let results: Vec<(String, ResultHackedFloat)> =
        map.into_iter().sorted_by_key(|key| key.0.clone()).collect();
    println!("{:#?}", results);
}

fn single_threaded() {
    // File hosts.txt must exist in the current path
    let mut map = HashMap::<String, Result>::with_capacity(10000);
    if let Ok(lines) = read_lines("./measurements.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines.flatten() {
            let (city, temp) = line.split(";").next_tuple().unwrap();
            let temp = temp.parse::<f64>().unwrap();
            if let Some(c) = map.get_mut(city) {
                c.min = f64::min(c.min, temp);
                c.max = f64::max(c.max, temp);
                let new_len = c.len + 1;
                c.mean = c.mean + ((temp - c.mean) / new_len as f64);
                c.len = new_len;
            } else {
                map.insert(
                    city.to_string(),
                    Result {
                        min: temp,
                        max: temp,
                        mean: temp,
                        len: 1,
                    },
                );
            }
        }
    }
    let results: Vec<(String, Result)> =
        map.into_iter().sorted_by_key(|key| key.0.clone()).collect();
    println!("{:#?}", results);
}

fn multi_process_rayon() {
    if let Ok(lines) = read_lines("./measurements.txt") {
        let results: HashMap<String, f64> = lines
            .flatten()
            .par_bridge()
            .fold(
                || HashMap::new(),
                |mut m: HashMap<String, f64>, l: String| {
                    let (city, temp) = l.split(";").next_tuple().unwrap();
                    let temp = temp.parse::<f64>().unwrap();
                    m.insert(city.to_string(), temp);
                    return m;
                },
            )
            .reduce(
                || HashMap::new(),
                |mut map1, map2| {
                    for (k, v) in map2.iter() {
                        if map1.contains_key(k) {
                            let cur_val = map1[k];
                            if cur_val > *v {
                                map1.insert(k.clone(), *v);
                            }
                        }
                    }
                    return map1;
                },
            );
        println!("{:#?}", results);
    }
}

fn just_read_file_rayon() {
    if let Ok(lines) = read_lines("./measurements.txt") {
        // Consumes the iterator, returns an (Optional) String
        lines.par_bridge().for_each(|_| ());
    }
}

fn just_read_file() {
    if let Ok(lines) = read_lines("./measurements.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines.flatten() {}
        ();
    }
}

fn fast_read_lines<P>(filename: P)
where
    P: AsRef<Path>,
{
    const buffer_size_bytes: usize = 2_000_000;
    let mut file = File::open(filename).unwrap();
    let mut buffer = [0; buffer_size_bytes];
    let mut line_count: u64 = 0;
    let mut overflow_buffer = [0u8; 100];
    let mut use_overflow = false;
    let mut overflow_len = 0;
    let mut overflow_count = 0;
    while let Ok(n) = file.read(&mut buffer[..]) {
        // Possible State:
        // 1) EOL is last Byte in buffer
        //      -- No need to store overflow
        //      -- Start next buffer as normal
        // 2) EOL is first Byte in buffer
        //      -- Doesn't happen on first iteration
        //      -- Overflow buffer must have data
        // 3) First and last bytes are normal
        //      -- Except for first iteration, will have data in overflow buffer
        //      -- Needs to store data into overflow buffer
        let mut line_start_ptr = 0;
        if use_overflow {
            // Find first EOL and concat
            let mut idx = 0;
            while buffer[idx] != EOL {
                idx += 1;
            }
            let line = str::from_utf8(&[&overflow_buffer[..overflow_len], &buffer[..idx]].concat()).unwrap().to_owned();
            let result = parse_line(&line);
            line_count += 1;
            line_start_ptr = idx + 1;
            use_overflow = false;
        }
        for idx in line_start_ptr..n {

            if buffer[idx] == EOL {
                let line = str::from_utf8(&buffer[line_start_ptr..idx]).unwrap();
                let result = parse_line(line);
                line_start_ptr = idx + 1;
                line_count += 1;
            }
            if idx == n - 1 && buffer[idx] != EOL {
                overflow_count += 1;
                use_overflow = true;
                overflow_len = n - line_start_ptr;
                overflow_buffer[..overflow_len].copy_from_slice(&buffer[line_start_ptr..]);
            }
        }
        if n == 0 {
            break;
        }
    }
    println!("Found lines {}", line_count);
    println!("Used overflow buffer {} times.", overflow_count);
}

fn parse_line(line: &str) -> (String, f64) {
    let (city, temp) = line.split(";").next_tuple().unwrap();
    let city = String::from(city);
    let temp: f64 = temp.parse::<f64>().unwrap();
    return (city, temp);
}

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
