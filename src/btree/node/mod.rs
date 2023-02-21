use crate::btree::node::leaf::LeafNodeRefMut;
use crate::btree::tuple::Tuple;
use crate::page;
use crate::page::Page;
use crate::schema::Schema;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Formatter;
use std::mem::size_of;
use tokio::sync::RwLockWriteGuard;

pub mod leaf;

pub const HEADER_SIZE: usize = size_of::<i32>();

#[derive(Debug, PartialEq)]
pub enum NodeType {
    Leaf = 0,
    Inner = 1,
}

impl Serialize for NodeType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            NodeType::Leaf => 0,
            NodeType::Inner => 1,
        };
        serializer.serialize_u16(value)
    }
}

impl<'de> Deserialize<'de> for NodeType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = NodeType;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a valid valid node type")
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(match v {
                    0 => NodeType::Leaf,
                    1 => NodeType::Inner,
                    _ => unreachable!(),
                })
            }
        }

        deserializer.deserialize_u16(Visitor)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Header {
    node_type: NodeType,
}

#[derive(Debug)]
pub enum NodeRefMut<'a> {
    Leaf(LeafNodeRefMut<'a>),
}

impl<'a> NodeRefMut<'a> {
    pub fn insert(&mut self, tuple: Tuple, schema: &Schema) {
        match self {
            NodeRefMut::Leaf(x) => x.insert(tuple, schema),
        }
    }

    pub fn from_page(page: &'a mut RwLockWriteGuard<Page>, schema: &Schema) -> Self {
        let buffer = page.buffer_mut();
        let start = page::HEADER_SIZE + HEADER_SIZE;
        let header: Header = bincode::deserialize(&buffer[page::HEADER_SIZE..start]).unwrap();
        match header.node_type {
            NodeType::Leaf => {
                NodeRefMut::Leaf(LeafNodeRefMut::from_buffer(&mut buffer[start..], schema))
            }
            NodeType::Inner => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_leaf_serialize() {
        let header = Header {
            node_type: NodeType::Leaf,
        };
        let serialized = bincode::serialize(&header).unwrap();
        assert_eq!(serialized, vec![0, 0]);
    }

    #[test]
    fn header_leaf_deserialize() {
        let raw = vec![0, 0];
        let header: Header = bincode::deserialize(&raw).unwrap();

        let expected = Header {
            node_type: NodeType::Leaf,
        };
        assert_eq!(header, expected);
    }

    #[test]
    fn header_inner_serialize() {
        let header = Header {
            node_type: NodeType::Inner,
        };
        let serialized = bincode::serialize(&header).unwrap();
        assert_eq!(serialized, vec![1, 0]);
    }

    #[test]
    fn header_inner_deserialize() {
        let raw = vec![1, 0];
        let header: Header = bincode::deserialize(&raw).unwrap();

        let expected = Header {
            node_type: NodeType::Inner,
        };
        assert_eq!(header, expected);
    }
}
