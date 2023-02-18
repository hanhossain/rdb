use serde::{Deserialize, Serialize};
use std::mem::size_of;

mod leaf;

const HEADER_SIZE: usize = size_of::<i32>();

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeType {
    Leaf,
    Inner,
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
        assert_eq!(serialized, vec![0, 0, 0, 0]);
    }

    #[test]
    fn header_leaf_deserialize() {
        let raw = vec![0, 0, 0, 0];
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
        assert_eq!(serialized, vec![1, 0, 0, 0]);
    }

    #[test]
    fn header_inner_deserialize() {
        let raw = vec![1, 0, 0, 0];
        let header: Header = bincode::deserialize(&raw).unwrap();

        let expected = Header {
            node_type: NodeType::Inner,
        };
        assert_eq!(header, expected);
    }
}
