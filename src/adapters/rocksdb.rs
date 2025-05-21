use std::sync::Arc;

use crate::bustle::*;
use rocksdb::*;

use crate::model::*;

pub struct RocksDbTable {
    database: Arc<DB>,
    _tempdir: tempfile::TempDir,
}

pub struct RocksDbHandle(Arc<DB>);

impl Collection for RocksDbTable {
    type Handle = RocksDbHandle;

    fn with_capacity(_: usize) -> Self {
        let tempdir = tempfile::Builder::new()
            .prefix("bench-rocksdb")
            .tempdir()
            .unwrap();

        let db = DB::open_default(tempdir.path()).unwrap();

        Self {
            database: Arc::new(db),
            _tempdir: tempdir,
        }
    }

    fn pin(&self) -> Self::Handle {
        RocksDbHandle(self.database.clone())
    }
}

impl Drop for RocksDbTable {
    fn drop(&mut self) {
        let _ = DB::destroy(&Options::default(), self._tempdir.path());
    }
}

impl CollectionHandle for RocksDbHandle {
    type Key = UserAddress;

    fn get(&mut self, key: &Self::Key) {
        self.0.get_pinned(key.as_bytes()).unwrap();
    }

    fn insert(&mut self, key: &Self::Key) {
        self.0.put(key.as_bytes(), VALUE_DATA).unwrap();
    }
}
