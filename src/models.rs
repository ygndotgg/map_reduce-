// use serde::{Deserialize, Serialize};

use std::collections::HashMap;
#[derive(Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub type MapFunction = fn(String, String) -> Vec<KeyValue>;

pub type ReduceFunction = fn(String, Vec<String>) -> String;

pub enum Report {
    MapDone {
        taskid: u32,
        files: HashMap<u32, String>,
    },
    ReducerDone {
        taskid: u32,
    },
    Exit,
}
