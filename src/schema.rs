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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Column {
            name: name.to_string(),
            data_type,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub primary_key: String,
}

impl Schema {
    pub fn new(columns: Vec<Column>, primary_key: &str) -> Self {
        Schema {
            columns,
            primary_key: primary_key.to_string(),
        }
    }

    pub fn primary_key_index(&self) -> usize {
        for i in 0..self.columns.len() {
            if &self.columns[i].name == &self.primary_key {
                return i;
            }
        }

        unreachable!()
    }

    pub fn tuple_size(&self) -> usize {
        self.columns.iter().map(|x| x.data_type.size()).sum()
    }
}
