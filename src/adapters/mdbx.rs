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

        let txn = db.begin_rw_txn().unwrap();
        let table = txn.create_table(None, TableFlags::DUP_SORT).unwrap();
        for (k, v) in [
            (b"key1", b"val1"),
            (b"key1", b"val2"),
            (b"key1", b"val3"),
            (b"key2", b"val1"),
            (b"key2", b"val2"),
            (b"key2", b"val3"),
            (b"key3", b"val1"),
            (b"key3", b"val2"),
            (b"key3", b"val3"),
        ] {
            txn.put(&table, k, v, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();

        let txn = db.begin_rw_txn().unwrap();
        let table = txn.open_table(None).unwrap();
        {
            let mut cur = txn.cursor(&table).unwrap();
            let iter = cur.iter_dup_of::<(), [u8; 4]>(b"key1");
            let vals = iter.map(|x| x.unwrap()).map(|(_, x)| x).collect::<Vec<_>>();
            assert_eq!(vals, vec![*b"val1", *b"val2", *b"val3"]);
        }
        txn.commit().unwrap();

        let txn = db.begin_rw_txn().unwrap();
        let table = txn.open_table(None).unwrap();
        for (k, v) in [(b"key1", Some(b"val2" as &[u8])), (b"key2", None)] {
            txn.del(&table, k, v).unwrap();
        }
        txn.commit().unwrap();

        let txn = db.begin_rw_txn().unwrap();
        let table = txn.open_table(None).unwrap();
        {
            let mut cur = txn.cursor(&table).unwrap();
            let iter = cur.iter_dup_of::<(), [u8; 4]>(b"key1");
            let vals = iter.map(|x| x.unwrap()).map(|(_, x)| x).collect::<Vec<_>>();
            assert_eq!(vals, vec![*b"val1", *b"val3"]);

            let iter = cur.iter_dup_of::<(), ()>(b"key2");
            assert_eq!(0, iter.count());
        }
        txn.commit().unwrap();

        Self(Arc::new(db))
    }

    fn pin(&self) -> Self::Handle {
        Self(self.0.clone())
    }
}

impl CollectionHandle for MdbxTable {
    type Key = UserAddress;

    fn get(&mut self, key: &Self::Key) -> bool {
        // let tx = self.0.begin_ro_txn().unwrap();
        // let table = tx.open_table(None).unwrap();

        // tx.get::<Vec<u8>>(&table, key.as_bytes()).unwrap().is_some()
        false
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        // let tx = self.0.begin_rw_txn().unwrap();
        // let table = tx.open_table(None).unwrap();
        
        // let result = tx.put(&table, key.as_bytes(), VALUE_DATA, WriteFlags::NO_OVERWRITE);
        // tx.commit().unwrap();

        // match result {
        //     Ok(()) => true,
        //     Err(Error::KeyExist) => false,
        //     _ => panic!()
        // }
        false
    }

    fn remove(&mut self, _key: &Self::Key) -> bool {
        unreachable!()
    }

    fn update(&mut self, _key: &Self::Key) -> bool {
        unreachable!()
    }
}
