//! Persisted secondary index segments: payload codec and in-memory replay state.

use std::collections::{BTreeSet, HashMap};

use crate::error::{DbError, FormatError, SchemaError};
use crate::schema::IndexKind;

pub const INDEX_PAYLOAD_VERSION_V1: u16 = 1;
pub const INDEX_PAYLOAD_VERSION_V2: u16 = 2;
pub const INDEX_PAYLOAD_VERSION: u16 = INDEX_PAYLOAD_VERSION_V2;

type IndexName = String;
type IndexKey = Vec<u8>;
type PkKey = Vec<u8>;
type IndexId = (u32, IndexName);
type UniqueIndex = HashMap<IndexKey, PkKey>;
type NonUniqueIndex = HashMap<IndexKey, BTreeSet<PkKey>>;

/// Index delta operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOp {
    Insert,
    Delete,
}

/// One index update entry (insert/update/delete).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub collection_id: u32,
    pub index_name: String,
    pub kind: IndexKind,
    pub op: IndexOp,
    pub index_key: Vec<u8>,
    pub pk_key: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct IndexState {
    unique: HashMap<IndexId, UniqueIndex>,
    non_unique: HashMap<IndexId, NonUniqueIndex>,
}

impl IndexState {
    pub fn apply(&mut self, entry: IndexEntry) -> Result<(), DbError> {
        match entry.kind {
            IndexKind::Unique => {
                let m = self
                    .unique
                    .entry((entry.collection_id, entry.index_name))
                    .or_default();
                match entry.op {
                    IndexOp::Insert => match m.get(&entry.index_key) {
                        None => {
                            m.insert(entry.index_key, entry.pk_key);
                            Ok(())
                        }
                        Some(existing) if *existing == entry.pk_key => Ok(()),
                        Some(_) => Err(DbError::Schema(SchemaError::UniqueIndexViolation)),
                    },
                    IndexOp::Delete => match m.get(&entry.index_key) {
                        None => Ok(()),
                        Some(existing) if *existing == entry.pk_key => {
                            m.remove(&entry.index_key);
                            Ok(())
                        }
                        Some(_) => Ok(()),
                    },
                }
            }
            IndexKind::NonUnique => {
                let m = self
                    .non_unique
                    .entry((entry.collection_id, entry.index_name))
                    .or_default();
                match entry.op {
                    IndexOp::Insert => {
                        m.entry(entry.index_key).or_default().insert(entry.pk_key);
                    }
                    IndexOp::Delete => {
                        if let Some(set) = m.get_mut(&entry.index_key) {
                            set.remove(&entry.pk_key);
                            if set.is_empty() {
                                m.remove(&entry.index_key);
                            }
                        }
                    }
                }
                Ok(())
            }
        }
    }

    pub fn unique_lookup(
        &self,
        collection_id: u32,
        index_name: &str,
        index_key: &[u8],
    ) -> Option<&[u8]> {
        self.unique
            .get(&(collection_id, index_name.to_string()))?
            .get(index_key)
            .map(|v| v.as_slice())
    }

    pub fn non_unique_lookup(
        &self,
        collection_id: u32,
        index_name: &str,
        index_key: &[u8],
    ) -> Option<Vec<Vec<u8>>> {
        let set = self
            .non_unique
            .get(&(collection_id, index_name.to_string()))?
            .get(index_key)?;
        Some(set.iter().cloned().collect())
    }

    pub(crate) fn entries_for_checkpoint(&self) -> Vec<IndexEntry> {
        let mut out = Vec::new();
        for ((collection_id, index_name), m) in &self.unique {
            for (index_key, pk_key) in m {
                out.push(IndexEntry {
                    collection_id: *collection_id,
                    index_name: index_name.clone(),
                    kind: IndexKind::Unique,
                    op: IndexOp::Insert,
                    index_key: index_key.clone(),
                    pk_key: pk_key.clone(),
                });
            }
        }
        for ((collection_id, index_name), m) in &self.non_unique {
            for (index_key, set) in m {
                for pk_key in set {
                    out.push(IndexEntry {
                        collection_id: *collection_id,
                        index_name: index_name.clone(),
                        kind: IndexKind::NonUnique,
                        op: IndexOp::Insert,
                        index_key: index_key.clone(),
                        pk_key: pk_key.clone(),
                    });
                }
            }
        }
        out
    }
}

pub fn encode_index_payload(entries: &[IndexEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&INDEX_PAYLOAD_VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        out.extend_from_slice(&e.collection_id.to_le_bytes());
        out.push(match e.kind {
            IndexKind::Unique => 1,
            IndexKind::NonUnique => 2,
        });
        out.push(match e.op {
            IndexOp::Insert => 1,
            IndexOp::Delete => 2,
        });
        encode_string(&mut out, &e.index_name);
        encode_bytes(&mut out, &e.index_key);
        encode_bytes(&mut out, &e.pk_key);
    }
    out
}

pub fn decode_index_payload(bytes: &[u8]) -> Result<Vec<IndexEntry>, DbError> {
    let mut cur = Cursor::new(bytes);
    let ver = cur.take_u16()?;
    if ver != INDEX_PAYLOAD_VERSION_V1 && ver != INDEX_PAYLOAD_VERSION_V2 {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: ver,
        }));
    }
    let n = cur.take_u32()? as usize;
    let mut v = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let collection_id = cur.take_u32()?;
        let kind_tag = cur.take_u8()?;
        let kind = match kind_tag {
            1 => IndexKind::Unique,
            2 => IndexKind::NonUnique,
            _ => {
                return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                    message: format!("unknown index kind tag {kind_tag}"),
                }))
            }
        };
        let op = if ver >= INDEX_PAYLOAD_VERSION_V2 {
            let op_tag = cur.take_u8()?;
            match op_tag {
                1 => IndexOp::Insert,
                2 => IndexOp::Delete,
                _ => {
                    return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                        message: format!("unknown index op tag {op_tag}"),
                    }))
                }
            }
        } else {
            IndexOp::Insert
        };
        let index_name = decode_string(&mut cur)?;
        let index_key = decode_bytes(&mut cur)?;
        let pk_key = decode_bytes(&mut cur)?;
        v.push(IndexEntry {
            collection_id,
            index_name,
            kind,
            op,
            index_key,
            pk_key,
        });
    }
    if cur.remaining() != 0 {
        return Err(DbError::Format(FormatError::InvalidCatalogPayload {
            message: "trailing bytes in index payload".to_string(),
        }));
    }
    Ok(v)
}

fn encode_string(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn decode_string(cur: &mut Cursor<'_>) -> Result<String, DbError> {
    let n = cur.take_u32()? as usize;
    if n == 0 {
        return Err(DbError::Format(FormatError::InvalidCatalogPayload {
            message: "empty index name".to_string(),
        }));
    }
    let bytes = cur.take_bytes(n)?;
    String::from_utf8(bytes).map_err(|_| {
        DbError::Format(FormatError::InvalidCatalogPayload {
            message: "invalid utf-8 in index name".to_string(),
        })
    })
}

fn encode_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn decode_bytes(cur: &mut Cursor<'_>) -> Result<Vec<u8>, DbError> {
    let n = cur.take_u32()? as usize;
    cur.take_bytes(n)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn take_u8(&mut self) -> Result<u8, DbError> {
        if self.pos >= self.bytes.len() {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let b = self.bytes[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn take_u16(&mut self) -> Result<u16, DbError> {
        if self.remaining() < 2 {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let v = u16::from_le_bytes([self.bytes[self.pos], self.bytes[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn take_u32(&mut self) -> Result<u32, DbError> {
        if self.remaining() < 4 {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let v = u32::from_le_bytes([
            self.bytes[self.pos],
            self.bytes[self.pos + 1],
            self.bytes[self.pos + 2],
            self.bytes[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn take_bytes(&mut self, n: usize) -> Result<Vec<u8>, DbError> {
        if self.remaining() < n {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_state_unique_insert_conflict_and_deletes() {
        let mut st = IndexState::default();

        // Insert new key.
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "u".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Insert,
            index_key: b"k".to_vec(),
            pk_key: b"pk1".to_vec(),
        })
        .unwrap();

        // Idempotent insert of same mapping.
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "u".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Insert,
            index_key: b"k".to_vec(),
            pk_key: b"pk1".to_vec(),
        })
        .unwrap();

        // Conflicting insert.
        assert!(st
            .apply(IndexEntry {
                collection_id: 1,
                index_name: "u".to_string(),
                kind: IndexKind::Unique,
                op: IndexOp::Insert,
                index_key: b"k".to_vec(),
                pk_key: b"pk2".to_vec(),
            })
            .is_err());

        // Delete with mismatched pk does nothing.
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "u".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Delete,
            index_key: b"k".to_vec(),
            pk_key: b"nope".to_vec(),
        })
        .unwrap();
        assert_eq!(st.unique_lookup(1, "u", b"k").unwrap(), b"pk1");

        // Delete with matching pk removes entry.
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "u".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Delete,
            index_key: b"k".to_vec(),
            pk_key: b"pk1".to_vec(),
        })
        .unwrap();
        assert!(st.unique_lookup(1, "u", b"k").is_none());

        // Delete of missing key is ok.
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "u".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Delete,
            index_key: b"missing".to_vec(),
            pk_key: b"pk".to_vec(),
        })
        .unwrap();
    }

    #[test]
    fn index_state_non_unique_insert_and_delete_removes_empty_set() {
        let mut st = IndexState::default();
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "n".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: b"k".to_vec(),
            pk_key: b"pk".to_vec(),
        })
        .unwrap();
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "n".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: b"k".to_vec(),
            pk_key: b"pk2".to_vec(),
        })
        .unwrap();
        // Order is not guaranteed; just ensure both are present.
        let mut got = st.non_unique_lookup(1, "n", b"k").unwrap();
        got.sort();
        assert_eq!(got, vec![b"pk".to_vec(), b"pk2".to_vec()]);

        // Delete one pk; set remains non-empty.
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "n".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Delete,
            index_key: b"k".to_vec(),
            pk_key: b"pk2".to_vec(),
        })
        .unwrap();
        assert_eq!(st.non_unique_lookup(1, "n", b"k").unwrap(), vec![b"pk".to_vec()]);

        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "n".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Delete,
            index_key: b"k".to_vec(),
            pk_key: b"pk".to_vec(),
        })
        .unwrap();
        assert!(st.non_unique_lookup(1, "n", b"k").is_none());

        // Delete when key is absent is ok.
        st.apply(IndexEntry {
            collection_id: 1,
            index_name: "n".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Delete,
            index_key: b"missing".to_vec(),
            pk_key: b"pk".to_vec(),
        })
        .unwrap();
    }

    #[test]
    fn index_payload_v1_decodes_without_op_tag() {
        // Manually craft a v1 payload: version, count, collection_id, kind_tag, then name/key/pk.
        let mut out = Vec::new();
        out.extend_from_slice(&INDEX_PAYLOAD_VERSION_V1.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes());
        out.push(1); // Unique

        // index_name = "idx"
        out.extend_from_slice(&(3u32.to_le_bytes()));
        out.extend_from_slice(b"idx");

        // index_key = b"k"
        out.extend_from_slice(&(1u32.to_le_bytes()));
        out.extend_from_slice(b"k");

        // pk_key = b"pk"
        out.extend_from_slice(&(2u32.to_le_bytes()));
        out.extend_from_slice(b"pk");

        let v = decode_index_payload(&out).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].op, IndexOp::Insert);
    }

    #[test]
    fn index_payload_rejects_unknown_kind_and_op_tags_and_trailing_bytes() {
        let base = IndexEntry {
            collection_id: 1,
            index_name: "idx".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Insert,
            index_key: b"k".to_vec(),
            pk_key: b"pk".to_vec(),
        };

        // Unknown kind tag.
        let mut bytes = encode_index_payload(&[base.clone()]);
        // overwrite kind tag at fixed offset: ver(2)+n(4)+cid(4)=10
        bytes[10] = 9;
        assert!(decode_index_payload(&bytes).is_err());

        // Unknown op tag (v2 only): ver(2)+n(4)+cid(4)+kind(1)=11
        let mut bytes = encode_index_payload(&[base.clone()]);
        bytes[11] = 9;
        assert!(decode_index_payload(&bytes).is_err());

        // Trailing bytes.
        let mut bytes = encode_index_payload(&[base]);
        bytes.extend_from_slice(b"x");
        assert!(decode_index_payload(&bytes).is_err());
    }

    #[test]
    fn index_payload_rejects_unsupported_version_and_truncated_fields() {
        // Unsupported version (also exercises the second half of the version check).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&999u16.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_index_payload(&bytes).is_err());

        // Truncated before u16 version.
        assert!(decode_index_payload(&[]).is_err());

        // Truncated before kind_tag u8.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&INDEX_PAYLOAD_VERSION_V2.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        assert!(decode_index_payload(&bytes).is_err());

        // Truncated during index_name bytes (exercise take_bytes unexpected eof).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&INDEX_PAYLOAD_VERSION_V2.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(1); // kind unique
        bytes.push(1); // op insert
        bytes.extend_from_slice(&5u32.to_le_bytes()); // name len=5
        bytes.extend_from_slice(b"x"); // only 1 byte provided
        assert!(decode_index_payload(&bytes).is_err());
    }
}
