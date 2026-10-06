use std::cell::UnsafeCell;
use std::env::consts::EXE_SUFFIX;
use std::io::{BufRead as _, BufReader};
use std::mem;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Once, RwLock};

use tigerbeetle as tb;

pub type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub const TEST_LEDGER: u32 = 10;
pub const TEST_CODE: u16 = 20;

// Singleton test database.
// This can be a OnceLock in Rust 1.70+, and LazyLock in 1.80.
pub fn get_test_db() -> &'static TestDb {
    struct OnceLock {
        once: Once,
        value: UnsafeCell<Option<TestDb>>,
    }

    unsafe impl Sync for OnceLock {}

    static TEST_DB: OnceLock = OnceLock {
        once: Once::new(),
        value: UnsafeCell::new(None),
    };

    let error_msg = "couldn't start test database";

    unsafe {
        TEST_DB.once.call_once(|| {
            *(&mut *TEST_DB.value.get()) = Some(TestDb::new().expect(error_msg));
        });

        (&*TEST_DB.value.get()).as_ref().expect(error_msg)
    }
}

pub struct TestDb {
    port: u16,
    // Keep the server's stdin handle open as long as the test process is running,
    // at which point the server will terminate.
    _server: Child,
}

pub fn tigerbeetle_bin() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../../tigerbeetle{EXE_SUFFIX}")
}

pub fn work_dir() -> &'static str {
    env!("CARGO_TARGET_TMPDIR")
}

impl TestDb {
    pub fn new() -> Result<TestDb> {
        // NB: There is one test database shared between all tests, and reused
        // between test runs. If the tests choose their IDs correctly there
        // should never be any collisions, and that one database should work
        // forever, just taking up a lot of space.
        let database_name = "0_0.testdb.tigerbeetle";

        if !Path::new(&format!("{}/{database_name}", work_dir())).try_exists()? {
            let status = Command::new(tigerbeetle_bin())
                .current_dir(work_dir())
                .args([
                    "format",
                    "--replica-count=1",
                    "--replica=0",
                    "--cluster=0",
                    database_name,
                ])
                .status()?;
            assert!(status.success());
        }

        let server = Self::start(&["--addresses=0", "--cache-grid=32MiB", database_name])?;

        Ok(server)
    }

    /// Create a unique development-mode server for a specific test.
    pub fn new_development(label: &str) -> Result<TestDb> {
        let database_name = format!("0_0.{label}.tigerbeetle");

        // Always start fresh for development instances.
        let _ = std::fs::remove_file(format!("{}/{database_name}", work_dir()));

        let status = Command::new(tigerbeetle_bin())
            .current_dir(work_dir())
            .args([
                "format",
                "--replica-count=1",
                "--replica=0",
                "--cluster=0",
                "--development",
                &database_name,
            ])
            .status()?;
        assert!(status.success());

        let server = Self::start(&["--addresses=0", "--development", &database_name])?;

        Ok(server)
    }

    pub fn start(args: &[&str]) -> Result<TestDb> {
        let mut server = Command::new(tigerbeetle_bin())
            .current_dir(work_dir())
            // magic address 0: tell us the port to use,
            // shutdown when stdin closes
            .args(["start"])
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        let server_stdout = mem::take(&mut server.stdout).unwrap();
        let mut server_stdout = BufReader::new(server_stdout);
        let mut first_line = String::new();
        server_stdout.read_line(&mut first_line)?;
        let port = first_line.trim().parse()?;

        Ok(TestDb {
            port,
            _server: server,
        })
    }

    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

// Only one database server should run at a time. Normal tests share a read
// lock; the eviction test takes a write lock so it runs exclusively.
pub static DB_LOCK: RwLock<()> = RwLock::new(());

/// Returns the client and a read guard that must be held for the test's
/// duration. The guard prevents the eviction test from running concurrently.
pub fn test_client() -> Result<(tb::Client, std::sync::RwLockReadGuard<'static, ()>)> {
    let guard = DB_LOCK.read().unwrap();
    let client = tb::Client::new(0, &get_test_db().address())?;
    Ok((client, guard))
}

pub fn assert_send<T: Send>(t: T) -> T {
    t
}
