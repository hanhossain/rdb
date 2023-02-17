use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum DataType {
    Int32,
    Int64,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Int32 => size_of::<i32>(),
            DataType::Int64 => size_of::<i64>(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
}
