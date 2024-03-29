use crate::btree::node;
use crate::btree::node::leaf::header::Header;
use crate::btree::tuple::Tuple;
use crate::page;
use crate::schema::Schema;
use serde::ser::SerializeTuple;
use serde::{Serialize, Serializer};
use std::fmt::Debug;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};

pub mod header;

#[derive(Debug, PartialEq)]
pub struct LeafNode {
    header: Header,
    tuples: Vec<Tuple>,
}

impl LeafNode {
    /// Calculate the max capacity for the schema.
    pub fn capacity(schema: &Schema) -> usize {
        let vec_count_size = size_of::<u64>();
        let data_size = page::DATA_SIZE - node::HEADER_SIZE - header::HEADER_SIZE - vec_count_size;
        data_size / schema.tuple_size()
    }

    fn deserialize_slice(buffer: &[u8], schema: &Schema) -> Self {
        let header: Header = bincode::deserialize(&buffer[..header::HEADER_SIZE]).unwrap();
        let tuple_size = schema.tuple_size();
        let mut tuples = Vec::new();

        for i in 0..header.count {
            let start = header::HEADER_SIZE + i * tuple_size;
            let end = start + tuple_size;
            let tuple = Tuple::deserialize_slice(&buffer[start..end], schema);
            tuples.push(tuple);
        }

        LeafNode { header, tuples }
    }

    pub(super) fn insert(&mut self, tuple: Tuple, schema: &Schema) {
        let key_index = schema.primary_key_index();
        let key = &tuple.columns[key_index];
        if let Err(index) = self
            .tuples
            .binary_search_by(|t| t.columns[key_index].partial_cmp(key).unwrap())
        {
            self.tuples.insert(index, tuple);
            self.header.count += 1;
        }
    }
}

impl Serialize for LeafNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_tuple(self.header.count + 1)?;
        s.serialize_element(&self.header)?;
        for tuple in &self.tuples {
            s.serialize_element(tuple)?;
        }
        s.end()
    }
}

#[derive(Debug)]
pub struct LeafNodeRefMut<'a> {
    node: LeafNode,
    buffer: &'a mut [u8],
}

impl<'a> LeafNodeRefMut<'a> {
    pub fn from_buffer(buffer: &'a mut [u8], schema: &Schema) -> Self {
        let node = LeafNode::deserialize_slice(buffer, schema);
        LeafNodeRefMut { node, buffer }
    }
}

impl<'a> Deref for LeafNodeRefMut<'a> {
    type Target = LeafNode;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl<'a> DerefMut for LeafNodeRefMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}

impl<'a> Drop for LeafNodeRefMut<'a> {
    fn drop(&mut self) {
        bincode::serialize_into(&mut self.buffer, &self.node).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::tuple;
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

        assert_eq!(LeafNode::capacity(&schema), 507);
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
                count: 0,
                previous: None,
                next: None,
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
                count: 0,
                previous: None,
                next: None,
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
                count: 0,
                previous: None,
                next: None,
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

    #[test]
    fn leaf_serialize_and_deserialize() {
        let schema = Schema {
            columns: vec![Column {
                name: String::from("c1"),
                data_type: DataType::Int32,
            }],
            primary_key: String::from("c1"),
        };
        let node = LeafNode {
            header: Header {
                count: 4,
                previous: Some(2),
                next: Some(3),
            },
            tuples: (0..4)
                .map(|x| Tuple {
                    columns: vec![tuple::Column::Int32(x)],
                })
                .collect(),
        };

        // leaf header: 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        // tuples: 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0
        let serialized = bincode::serialize(&node).unwrap();
        assert_eq!(
            serialized,
            vec![
                4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0
            ]
        );

        let deserialized = LeafNode::deserialize_slice(&serialized, &schema);
        assert_eq!(deserialized, node);
    }
}
