use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Formatter;
use std::mem::size_of;

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
