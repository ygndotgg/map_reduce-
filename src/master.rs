use std::collections::HashMap;

pub enum Phase {
    Map,
    Reduce,
    Done,
}

pub struct Master {
    map_task: HashMap<usize, String>,
    reduce_task: HashMap<usize, String>,
    phase: Phase,
    n_reduce: u32,
    input_files: Vec<String>,
    map_outputs: HashMap<usize, HashMap<usize, String>>,
}


impl Master {
    pub fn new() -> Master {
        Master {
            map_task: HashMap::new(),
            reduce_task: HashMap::new(),
            phase: Phase::Map,
            n_reduce: 2,
            input_files: vec!["abc.txt".to_string(), "bbc.txt".to_string()],
            map_outputs: HashMap::new(),
        }
    }

    pub fn map_task_adder(&self) -> HashMap<usize, String> {
        let mut hs = HashMap::new();
        for (i, file) in self.input_files.iter().enumerate() {
            hs.insert(i, file.clone());
        }
        hs
    }
}




pub fn main_run() {
    let mut a = Master::new();
    a.map_task = a.map_task_adder();
}
