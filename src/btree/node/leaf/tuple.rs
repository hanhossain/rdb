use crate::schema::{DataType, Schema};
use serde::ser::SerializeTuple;
use serde::{Serialize, Serializer};

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Column {
    Int32(i32),
    Int64(i64),
}

impl Column {
    pub fn deserialize_slice(slice: &[u8], data_type: DataType) -> Self {
        match data_type {
            DataType::Int32 => Column::Int32(i32::from_ne_bytes(slice.try_into().unwrap())),
            DataType::Int64 => Column::Int64(i64::from_ne_bytes(slice.try_into().unwrap())),
        }
    }
}

impl Serialize for Column {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Column::Int32(x) => serializer.serialize_i32(*x),
            Column::Int64(x) => serializer.serialize_i64(*x),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Tuple {
    pub columns: Vec<Column>,
}

impl Tuple {
    pub fn deserialize_slice(slice: &[u8], schema: &Schema) -> Self {
        let mut start = 0;
        let mut columns = Vec::new();

        for column_schema in &schema.columns {
            let end = start + column_schema.data_type.size();
            let column = Column::deserialize_slice(&slice[start..end], column_schema.data_type);
            columns.push(column);
            start = end;
        }

        Tuple { columns }
    }
}

impl Serialize for Tuple {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_tuple(self.columns.len())?;
        for column in &self.columns {
            s.serialize_element(column)?;
        }
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;

    #[test]
    fn column_serialize() {
        let columns = vec![
            (Column::Int32(42), vec![42, 0, 0, 0]),
            (Column::Int64(64), vec![64, 0, 0, 0, 0, 0, 0, 0]),
        ];
        for (column, expected) in columns {
            let serialized = bincode::serialize(&column).unwrap();
            assert_eq!(serialized, expected);
        }
    }

    #[test]
    fn column_deserialize() {
        let input = vec![
            (vec![42, 0, 0, 0], DataType::Int32, Column::Int32(42)),
            (
                vec![64, 0, 0, 0, 0, 0, 0, 0],
                DataType::Int64,
                Column::Int64(64),
            ),
        ];
        for (data, data_type, expected) in input {
            let column = Column::deserialize_slice(&data, data_type);
            assert_eq!(column, expected);
        }
    }

    #[test]
    fn tuple_serialize() {
        let tuple = Tuple {
            columns: vec![Column::Int32(42), Column::Int64(64)],
        };
        let serialized = bincode::serialize(&tuple).unwrap();
        assert_eq!(serialized, vec![42, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn tuple_deserialize() {
        let data = vec![42, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0];
        let schema = Schema {
            columns: vec![
                schema::Column {
                    name: String::from("i32"),
                    data_type: DataType::Int32,
                },
                schema::Column {
                    name: String::from("i64"),
                    data_type: DataType::Int64,
                },
            ],
            primary_key: String::from("i32"),
        };
        let deserialized = Tuple::deserialize_slice(&data, &schema);
        let expected = Tuple {
            columns: vec![Column::Int32(42), Column::Int64(64)],
        };
        assert_eq!(deserialized, expected);
    }
}
