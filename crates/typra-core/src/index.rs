//! Persisted secondary index segments: payload codec and in-memory replay state.

use std::collections::{BTreeSet, HashMap};

use crate::error::{DbError, FormatError, SchemaError};
use crate::schema::IndexKind;

pub const INDEX_PAYLOAD_VERSION_V1: u16 = 1;
pub const INDEX_PAYLOAD_VERSION: u16 = INDEX_PAYLOAD_VERSION_V1;

type IndexName = String;
type IndexKey = Vec<u8>;
type PkKey = Vec<u8>;
type IndexId = (u32, IndexName);
type UniqueIndex = HashMap<IndexKey, PkKey>;
type NonUniqueIndex = HashMap<IndexKey, BTreeSet<PkKey>>;

/// One index update entry (insert path only in 0.7.0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub collection_id: u32,
    pub index_name: String,
    pub kind: IndexKind,
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
                match m.get(&entry.index_key) {
                    None => {
                        m.insert(entry.index_key, entry.pk_key);
                        Ok(())
                    }
                    Some(existing) if *existing == entry.pk_key => Ok(()),
                    Some(_) => Err(DbError::Schema(SchemaError::UniqueIndexViolation)),
                }
            }
            IndexKind::NonUnique => {
                let m = self
                    .non_unique
                    .entry((entry.collection_id, entry.index_name))
                    .or_default();
                m.entry(entry.index_key).or_default().insert(entry.pk_key);
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
        encode_string(&mut out, &e.index_name);
        encode_bytes(&mut out, &e.index_key);
        encode_bytes(&mut out, &e.pk_key);
    }
    out
}

pub fn decode_index_payload(bytes: &[u8]) -> Result<Vec<IndexEntry>, DbError> {
    let mut cur = Cursor::new(bytes);
    let ver = cur.take_u16()?;
    if ver != INDEX_PAYLOAD_VERSION_V1 {
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
        let index_name = decode_string(&mut cur)?;
        let index_key = decode_bytes(&mut cur)?;
        let pk_key = decode_bytes(&mut cur)?;
        v.push(IndexEntry {
            collection_id,
            index_name,
            kind,
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
