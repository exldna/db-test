use std::sync::Arc;

use crate::bustle::*;
use libmdbx::*;

use crate::model::*;

pub struct MdbxTable{
    database: Arc<Database<NoWriteMap>>,
    _tempdir: tempfile::TempDir,
}

pub struct MdbxHandle(Arc<Database<NoWriteMap>>);

impl Collection for MdbxTable {
    type Handle = MdbxHandle;

    fn with_capacity(_: usize) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let database = Database::open(&tempdir).unwrap();

        // let geometry = Geometry {
        //     size: Some(0..(capacity * 16).next_power_of_two()),
        //     growth_step: Some(1024 * 1024),
        //     shrink_threshold: None,
        //     page_size: Some(PageSize::Set(4096)),
        // };

        // let db = Database::open_with_options()
        //     .set_geometry(geometry)
        //     .set_sync_mode(SyncMode::Durable)
        //     .open(dir.path())
        //     .unwrap();

        Self {
            database: Arc::new(database), 
            _tempdir: tempdir,
        }
    }

    fn pin(&self) -> Self::Handle {
        MdbxHandle(self.database.clone())
    }
}

impl CollectionHandle for MdbxHandle {
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
}
