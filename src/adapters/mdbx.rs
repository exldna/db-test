use std::sync::Arc;

use bustle::*;
use libmdbx::*;

use crate::model::*;

type Database = libmdbx::Database<NoWriteMap>;

pub struct MdbxTable(Arc<Database>);

impl Collection for MdbxTable {
    type Handle = Self;

    fn with_capacity(_: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir).unwrap();

        let geometry = Geometry {
            size: Some(0..(capacity * 16).next_power_of_two()),
            growth_step: Some(1024 * 1024),
            shrink_threshold: None,
            page_size: Some(PageSize::Set(4096)),
        };

        let db = Database::new()
            .set_geometry(geometry)
            .set_sync_mode(SyncMode::Durable)
            .open(dir.path())
            .unwrap();

        Self(Arc::new(db))
    }

    fn pin(&self) -> Self::Handle {
        Self(self.0.clone())
    }
}

impl CollectionHandle for MdbxTable {
    type Key = UserAddress;

    fn get(&mut self, key: &Self::Key) -> bool {
        let txn = self.0.begin_ro_txn().unwrap();
        let table = txn.open_table(None).unwrap();

        txn.get::<Vec<u8>>(&table, key.as_bytes()).unwrap().is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        let txn = self.0.begin_rw_txn().unwrap();
        let table = txn.open_table(None).unwrap();

        let result = txn.put(&table, key.as_bytes(), VALUE_DATA, WriteFlags::empty());
        txn.commit().unwrap();
        result.is_ok()
    }

    fn remove(&mut self, _key: &Self::Key) -> bool {
        unreachable!()
    }

    fn update(&mut self, _key: &Self::Key) -> bool {
        unreachable!()
    }
}
