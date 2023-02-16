use serde::de::SeqAccess;
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Formatter;

#[derive(Debug, Eq, PartialEq)]
pub struct Header {
    previous: Option<u64>,
    next: Option<u64>,
}

impl Serialize for Header {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_tuple(2)?;
        s.serialize_element(&self.previous.unwrap_or(0))?;
        s.serialize_element(&self.next.unwrap_or(0))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Header;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a valid leaf node header")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let previous = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("looking for tuple of size 2"))?;
                let next = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("looking for tuple of size 2"))?;
                Ok(Header {
                    previous: if previous == 0 { None } else { Some(previous) },
                    next: if next == 0 { None } else { Some(next) },
                })
            }
        }

        deserializer.deserialize_tuple(2, Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_serialize() {
        let header = Header {
            previous: Some(1),
            next: Some(2),
        };
        let serialized = bincode::serialize(&header).unwrap();
        assert_eq!(
            serialized,
            vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn header_serialize_and_deserialize() {
        let header = Header {
            previous: Some(1),
            next: Some(2),
        };
        let serialized = bincode::serialize(&header).unwrap();
        let deserialized: Header = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, header);
    }

    #[test]
    fn header_serialize_and_deserialize_with_defaults() {
        let header = Header {
            previous: None,
            next: None,
        };

        let serialized = bincode::serialize(&header).unwrap();
        assert_eq!(serialized, vec![0; 16]);

        let deserialized: Header = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, header);
    }
}
