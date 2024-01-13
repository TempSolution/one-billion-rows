use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::str::FromStr;
use std::string::ParseError;

use ahash::HashMapExt;
use itertools::Itertools;

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
    just_read_file();
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

fn just_read_file() {
    if let Ok(lines) = read_lines("./measurements.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines.flatten() {}
    }
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
