use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

use itertools::Itertools;

#[derive(Debug)]
struct Result {
    min: f64,
    max: f64,
    mean: f64,
    len: u32,
}

fn main() {
    // File hosts.txt must exist in the current path
    let mut map = HashMap::<String, Result>::new();
    if let Ok(lines) = read_lines("./measurements.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines.flatten() {
            let (city, temp) = line.split(";").next_tuple().unwrap();
            let temp = temp.parse::<f64>().unwrap();
            if map.contains_key(city) {
                let c = map.get_mut(city).unwrap();
                if temp < c.min {
                    c.min = temp;
                }
                if temp > c.max {
                    c.max = temp;
                }
                let new_len = c.len + 1;
                c.mean = ((c.mean * c.len as f64) + temp) / new_len as f64;
                c.len = new_len;
            } else {
                map.insert(city.to_string(), Result{min: temp, max: temp, mean: temp, len: 1});
            }
        }
    }
    let results: Vec<(String, Result)> = map.into_iter().sorted_by_key(|key| key.0.clone()).collect();
    println!("{:#?}", results);
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
