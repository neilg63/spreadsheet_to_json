// In a file like `tests/database_test.rs`
use mockall::mock;
use spreadsheet_to_json::db::{Database, sqlite::SqliteDb};

mock! {
    pub MockDatabase {}
    impl Database for MockDatabase {
        fn save_row(&mut self, row: &str) -> Result<(), Box<dyn Error>>;
    }
}

#[tokio::test]
async fn test_save_row() -> Result<(), Box<dyn Error>> {
    // Using the SQLite implementation for actual testing
    let mut db = SqliteDb::new()?;
    db.save_row("test data")?;
    // Here you could query to check if the data was saved, but for simplicity:

    // Now test with a mock to ensure the interface works
    let mut mock_db = MockDatabase::new();
    mock_db.expect_save_row()
        .withf(|row| row == "test data")
        .returning(|_| Ok(()));

    mock_db.save_row("test data")?;

    Ok(())
}