use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::DbError;

pub trait Store {
    fn len(&self) -> Result<u64, DbError>;
    fn is_empty(&self) -> Result<bool, DbError> {
        Ok(self.len()? == 0)
    }
    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError>;
    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError>;
    fn sync(&mut self) -> Result<(), DbError>;
}

// In 0.2.x this is intentionally internal scaffolding.
// The public API should not expose storage mechanics yet.
pub struct FileStore {
    file: File,
}

impl FileStore {
    pub fn new(file: File) -> Self {
        Self { file }
    }
}

impl Store for FileStore {
    fn len(&self) -> Result<u64, DbError> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), DbError> {
        self.file.sync_all()?;
        Ok(())
    }
}

pub struct StorageEngine;
