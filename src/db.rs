use std::error::Error;

pub trait Database {
    fn save_row(&mut self, row: &str) -> Result<(), Box<dyn Error>>;
}

// Example implementation for SQLite
#[cfg(test)]
mod sqlite {
    use super::Database;
    use std::error::Error;
    use rusqlite::{Connection, Result};

    pub struct SqliteDb(Connection);

    impl SqliteDb {
        pub fn new() -> Result<Self> {
            let conn = Connection::open_in_memory()?;
            conn.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, data TEXT NOT NULL)", [])?;
            Ok(SqliteDb(conn))
        }
    }

    impl Database for SqliteDb {
        fn save_row(&mut self, row: &str) -> Result<(), Box<dyn Error>> {
            self.0.execute("INSERT INTO test_table (data) VALUES (?1)", [row])?;
            Ok(())
        }
    }
}
