use crate::page;
use crate::schema::Schema;

mod header;
mod tuple;

#[derive(Debug)]
pub struct LeafNode {
    header: header::Header,
    tuples: Vec<tuple::Tuple>,
}

impl LeafNode {
    /// Calculate the max capacity for the schema.
    pub fn capacity(schema: &Schema) -> usize {
        let data_size = page::DATA_SIZE - header::HEADER_SIZE;
        let tuple_data_size: usize = schema.columns.iter().map(|x| x.data_type.size()).sum();
        data_size / tuple_data_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, DataType};

    #[test]
    fn leaf_capacity() {
        let schema = Schema {
            columns: vec![
                Column {
                    name: String::from("c1"),
                    data_type: DataType::Int32,
                },
                Column {
                    name: String::from("c2"),
                    data_type: DataType::Int32,
                },
            ],
        };

        // (page size - page header - header) / (c1 + c2);
        assert_eq!(LeafNode::capacity(&schema), 509);
    }
}
