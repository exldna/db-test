use std::sync::Arc;

use crate::bustle::*;
use libmdbx::*;

use crate::model::*;

pub struct MdbxTable {
    database: Arc<Database<NoWriteMap>>,
    _tempdir: tempfile::TempDir,
}

pub struct MdbxHandle(Arc<Database<NoWriteMap>>);

impl Collection for MdbxTable {
    type Handle = MdbxHandle;

    fn with_capacity(_: usize) -> Self {
        let tempdir = tempfile::Builder::new()
            .prefix("bench-mdbx")
            .tempdir()
            .unwrap();

        let database = Database::open(&tempdir).unwrap();

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

    fn get(&mut self, key: &Self::Key) {
        let txn = self.0.begin_ro_txn().unwrap();
        let table = txn.open_table(None).unwrap();

        txn.get::<Vec<u8>>(&table, key.as_bytes()).unwrap();
    }

    fn insert(&mut self, key: &Self::Key) {
        let txn = self.0.begin_rw_txn().unwrap();
        let table = txn.open_table(None).unwrap();

        txn.put(&table, key.as_bytes(), VALUE_DATA, WriteFlags::empty())
            .unwrap();
        txn.commit().unwrap();
    }
}
