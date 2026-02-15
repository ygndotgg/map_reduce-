// use serde::{Deserialize, Serialize};
type MapFunction = fn(String, String) -> Vec<KeyValue>;
type ReduceFunction = fn(String, Vec<String>) -> String;

pub struct KeyValue {
    pub key: String,
    pub value: String,
}
