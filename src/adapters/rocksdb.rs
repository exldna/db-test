use std::sync::Arc;

use bustle::*;
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

    fn get(&mut self, key: &Self::Key) -> bool {
        self.0.get(key.as_bytes()).unwrap().is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        let ret = self.0.get(key.as_bytes()).unwrap().is_none();
        self.0.put(key.as_bytes(), VALUE_DATA).unwrap();
        ret
    }

    fn remove(&mut self, _key: &Self::Key) -> bool {
        unimplemented!()
    }

    fn update(&mut self, _key: &Self::Key) -> bool {
        unimplemented!()
    }
}
