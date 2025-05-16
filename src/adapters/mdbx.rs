use std::sync::Arc;

use bustle::*;
use libmdbx::*;

use crate::model::*;

type Database = libmdbx::Database<NoWriteMap>;

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "libmdbx::Database");
        Ok(())
    }
}

#[derive(Debug)]
pub struct MdbxTable(Arc<Database>);

impl Collection for MdbxTable {
    type Handle = Self;

    fn with_capacity(_: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir).unwrap();
        Self(Arc::new(db))
    }

    fn pin(&self) -> Self::Handle {
        Self(self.0.clone())
    }
}

impl CollectionHandle for MdbxTable {
    type Key = UserAddress;

    #[tracing::instrument(level = "trace")]
    fn get(&mut self, key: &Self::Key) -> bool {
        let tx = self.0.begin_ro_txn().unwrap();
        let table = tx.open_table(None).unwrap();

        tx.get::<Vec<u8>>(&table, key.as_bytes()).unwrap().is_some()
    }

    #[tracing::instrument(level = "trace")]
    fn insert(&mut self, key: &Self::Key) -> bool {
        let tx = self.0.begin_rw_txn().unwrap();
        let table = tx.open_table(None).unwrap();
        
        let result = tx.put(&table, key.as_bytes(), VALUE_DATA, WriteFlags::NO_OVERWRITE);
        tx.commit().unwrap();

        match result {
            Ok(()) => true,
            Err(Error::KeyExist) => false,
            _ => panic!()
        }
    }

    fn remove(&mut self, _key: &Self::Key) -> bool {
        unreachable!()
    }

    fn update(&mut self, _key: &Self::Key) -> bool {
        unreachable!()
    }
}
