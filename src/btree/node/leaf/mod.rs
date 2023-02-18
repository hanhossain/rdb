use crate::btree::node::leaf::header::Header;
use crate::btree::node::leaf::tuple::Tuple;
use crate::page;
use crate::schema::Schema;

mod header;
mod tuple;

#[derive(Debug)]
pub struct LeafNode {
    header: Header,
    tuples: Vec<Tuple>,
}

impl LeafNode {
    /// Calculate the max capacity for the schema.
    pub fn capacity(schema: &Schema) -> usize {
        let data_size = page::DATA_SIZE - header::HEADER_SIZE;
        let tuple_data_size: usize = schema.columns.iter().map(|x| x.data_type.size()).sum();
        data_size / tuple_data_size
    }

    pub fn insert(&mut self, tuple: Tuple, schema: &Schema) {
        let key_index = schema.primary_key_index();
        let key = &tuple.columns[key_index];
        if let Err(index) = self
            .tuples
            .binary_search_by(|t| t.columns[key_index].partial_cmp(key).unwrap())
        {
            self.tuples.insert(index, tuple);
        }
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
            primary_key: String::from("c1"),
        };

        // (page size - page header - header) / (c1 + c2);
        assert_eq!(LeafNode::capacity(&schema), 509);
    }

    #[test]
    fn leaf_insert_single_column() {
        let schema = Schema {
            columns: vec![Column {
                name: String::from("c1"),
                data_type: DataType::Int32,
            }],
            primary_key: String::from("c1"),
        };

        let mut node = LeafNode {
            header: Header {
                next: None,
                previous: None,
            },
            tuples: Vec::new(),
        };

        node.insert(
            Tuple {
                columns: vec![tuple::Column::Int32(1)],
            },
            &schema,
        );

        assert_eq!(
            node.tuples,
            vec![Tuple {
                columns: vec![tuple::Column::Int32(1)]
            }]
        );
    }

    #[test]
    fn leaf_insert_multiple_with_single_column() {
        let schema = Schema {
            columns: vec![Column {
                name: String::from("c1"),
                data_type: DataType::Int32,
            }],
            primary_key: String::from("c1"),
        };
        let mut node = LeafNode {
            header: Header {
                next: None,
                previous: None,
            },
            tuples: Vec::new(),
        };

        let input = [1, 3, 0, 2, 0];
        for i in input {
            node.insert(
                Tuple {
                    columns: vec![tuple::Column::Int32(i)],
                },
                &schema,
            );
        }

        let expected: Vec<_> = (0..4)
            .map(|x| Tuple {
                columns: vec![tuple::Column::Int32(x)],
            })
            .collect();

        assert_eq!(node.tuples, expected);
    }

    #[test]
    fn leaf_insert_multiple_with_multiple_columns() {
        let schema = Schema {
            columns: vec![
                Column {
                    name: String::from("c1"),
                    data_type: DataType::Int32,
                },
                Column {
                    name: String::from("c2"),
                    data_type: DataType::Int64,
                },
            ],
            primary_key: String::from("c1"),
        };
        let mut node = LeafNode {
            header: Header {
                next: None,
                previous: None,
            },
            tuples: Vec::new(),
        };

        let input = [1, 3, 0, 2, 0];
        for i in input {
            node.insert(
                Tuple {
                    columns: vec![tuple::Column::Int32(i), tuple::Column::Int64(-1)],
                },
                &schema,
            );
        }

        let expected: Vec<_> = (0..4)
            .map(|x| Tuple {
                columns: vec![tuple::Column::Int32(x), tuple::Column::Int64(-1)],
            })
            .collect();

        assert_eq!(node.tuples, expected);
    }
}
