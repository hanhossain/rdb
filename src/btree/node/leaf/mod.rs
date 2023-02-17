mod header;
mod tuple;

#[derive(Debug)]
pub struct LeafNode {
    header: header::Header,
    tuples: Vec<tuple::Tuple>,
}
