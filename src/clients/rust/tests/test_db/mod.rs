use std::cell::UnsafeCell;
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
            *(&mut *TEST_DB.value.get()) =
                Some(TestDb::new("TB_ADDRESS", "testdb").expect(error_msg));
        });

        (&*TEST_DB.value.get()).as_ref().expect(error_msg)
    }
}

pub struct TestDb {
    port: u16,
    // Keep the server's stdin handle open as long as the test process is running,
    // at which point the server will terminate.
    _server: Option<Child>,
}

impl TestDb {
    /// Create a unique development-mode server for a specific test.
    pub fn new(port_env_var: &str, label_new: &str) -> Result<TestDb> {
        if let Ok(port_existing) = std::env::var(port_env_var) {
            return Ok(TestDb {
                port: port_existing.parse()?,
                _server: None,
            });
        }

        // We should only ever create databases for local tests, never in CI.
        // In CI, always rely on TigerBeetle servers started and cleaned-up by the zig CI code.
        // Specifying the tigerbeetle binary is useful for running tests locally during development.
        let tigerbeetle_bin = match std::env::var("TIGERBEETLE_BINARY") {
            Ok(tigerbeetle_bin) => tigerbeetle_bin,
            Err(_) => panic!(
                "Set either the {} or TIGERBEETLE_BINARY environment variable to execute tests.",
                port_env_var
            ),
        };

        let work_dir = env!("CARGO_TARGET_TMPDIR");
        let database_name = format!("0_0.{label_new}.tigerbeetle");
        let database_path = format!("{}/{database_name}", work_dir);

        if Path::new(&database_path).try_exists()? {
            std::fs::remove_file(Path::new(&database_path))
                .expect("could not remove old database file");
        }

        let status = Command::new(tigerbeetle_bin.as_str())
            .current_dir(work_dir)
            .args([
                "format",
                "--replica-count=1",
                "--replica=0",
                "--cluster=0",
                database_name.as_str(),
            ])
            .status()?;
        assert!(status.success());

        let mut server = Command::new(tigerbeetle_bin.as_str())
            .current_dir(work_dir)
            // Magic address 0: tell us the port to use, shutdown when stdin closes.
            .args(&["start", "--addresses=0", "--development", &database_name])
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
            _server: Some(server),
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
