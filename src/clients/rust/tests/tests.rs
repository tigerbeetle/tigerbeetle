use std::env;
use std::env::consts::EXE_SUFFIX;
use std::io::{BufRead as _, BufReader};
use std::mem;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::sync::{Mutex, MutexGuard};

use futures::executor::block_on;

use tigerbeetle as tb;

// Using the std hasher for rng to avoid the dep on rand crate.
fn random_u64() -> u64 {
    std::hash::Hasher::finish(&std::hash::BuildHasher::build_hasher(
        &std::collections::hash_map::RandomState::new(),
    ))
}

fn random_u128() -> u128 {
    let a = random_u64();
    let b = random_u64();
    let a = a as u128;
    let b = b as u128;
    a | b << 64
}

// Prevent multiple tests from running in parallel -
// tests run a full tigerbeetle and that's a lot of resources.
static TEST_MUTEX: Mutex<()> = Mutex::new(());

struct TestHarness {
    tigerbeetle_bin: String,
    temp_dir: PathBuf,
    server: Option<Child>,
    // nb: needs to be dropped last
    mutex_guard: Option<MutexGuard<'static, ()>>,
}

impl TestHarness {
    fn new(name: &str) -> anyhow::Result<TestHarness> {
        let mutex_guard = Some(TEST_MUTEX.lock().expect("nopoison"));

        let manifest_dir = env!("CARGO_MANIFEST_DIR");

        let tigerbeetle_bin = format!("{manifest_dir}/../../../tigerbeetle{EXE_SUFFIX}");

        let work_dir = env!("CARGO_TARGET_TMPDIR");
        let test_dir = format!("test-{name}-{}", random_u64());
        let temp_dir = format!("{work_dir}/{test_dir}");
        let temp_dir = PathBuf::from(temp_dir);
        std::fs::create_dir_all(&temp_dir)?;

        Ok(TestHarness {
            tigerbeetle_bin,
            temp_dir,
            server: None,
            mutex_guard,
        })
    }

    fn prepare_database(&self) -> anyhow::Result<()> {
        let mut cmd = Command::new(&self.tigerbeetle_bin);
        cmd.current_dir(&self.temp_dir);
        cmd.args([
            "format",
            "--replica-count=1",
            "--replica=0",
            "--cluster=0",
            "0_0.tigerbeetle",
        ]);
        let status = cmd.status()?;

        assert!(status.success());

        Ok(())
    }

    fn serve(&mut self) -> anyhow::Result<u16> {
        assert!(self.server.is_none());

        let mut cmd = Command::new(&self.tigerbeetle_bin);
        cmd.current_dir(&self.temp_dir);
        cmd.args([
            "start",
            "--addresses=0", // tell us the port to use
            "0_0.tigerbeetle",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
        let mut child = cmd.spawn()?;
        let child_stdout = mem::take(&mut child.stdout).unwrap();
        let mut child_stdout = BufReader::new(child_stdout);
        let mut first_line = String::new();
        child_stdout.read_line(&mut first_line)?;
        let port_number = first_line.trim().parse()?;

        self.server = Some(child);

        Ok(port_number)
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        if let Some(mut child) = self.server.take() {
            child.kill().expect("kill");
            let _ = child.wait().expect("wait");
        }
        let _ = std::fs::remove_dir_all(&self.temp_dir);
        if let Some(mutex_guard) = self.mutex_guard.take() {
            drop(mutex_guard)
        }
    }
}

fn server_and_client() -> anyhow::Result<(TestHarness, tb::Client)> {
    let mut harness = TestHarness::new("smoke")?;
    harness.prepare_database()?;
    let port_number = harness.serve()?;

    let address = &format!("127.0.0.1:{}", port_number);
    let client = tb::Client::new(0, address)?;

    Ok((harness, client))
}

const TEST_LEDGER: u32 = 10;
const TEST_CODE: u16 = 20;

#[test]
fn smoke() -> anyhow::Result<()> {
    let account_id1 = 1;
    let account_id2 = 2;
    let transfer_id1 = 3;

    let account_id2_user_data_32 = 4;
    let transfer_id1_user_data_32 = 5;

    block_on(async {
        let (_harness, client) = server_and_client()?;

        {
            let result = client
                .create_accounts(&[
                    tb::Account {
                        id: account_id1,
                        debits_pending: 0,
                        debits_posted: 0,
                        credits_pending: 0,
                        credits_posted: 0,
                        user_data_128: 0,
                        user_data_64: 0,
                        user_data_32: 0,
                        reserved: tb::Reserved::default(),
                        ledger: TEST_LEDGER,
                        code: TEST_CODE,
                        flags: tb::AccountFlags::History,
                        timestamp: 0,
                    },
                    tb::Account {
                        id: account_id2,
                        debits_pending: 0,
                        debits_posted: 0,
                        credits_pending: 0,
                        credits_posted: 0,
                        user_data_128: 0,
                        user_data_64: 0,
                        user_data_32: account_id2_user_data_32,
                        reserved: tb::Reserved::default(),
                        ledger: TEST_LEDGER,
                        code: TEST_CODE,
                        flags: tb::AccountFlags::History,
                        timestamp: 0,
                    },
                ])
                .await?;

            assert_eq!(result.len(), 2);

            assert_eq!(result[0], tb::CreateAccountResult::Ok);
            assert_eq!(result[1], tb::CreateAccountResult::Ok);
        }

        {
            let result = client
                .create_transfers(&[tb::Transfer {
                    id: transfer_id1,
                    debit_account_id: account_id1,
                    credit_account_id: account_id2,
                    amount: 10,
                    pending_id: 0,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: transfer_id1_user_data_32,
                    timeout: 0,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    flags: tb::TransferFlags::default(),
                    timestamp: 0,
                }])
                .await?;

            assert_eq!(result.len(), 1);
            assert_eq!(result[0], tb::CreateTransferResult::Ok);
        }

        {
            let result = client.lookup_accounts(&[account_id1, account_id2]).await?;

            assert_eq!(result.len(), 2);
            let res_account1 = result[0].unwrap();
            let res_account2 = result[1].unwrap();

            assert_eq!(res_account1.id, account_id1);
            assert_eq!(res_account1.debits_posted, 10);
            assert_eq!(res_account1.credits_posted, 0);
            assert_eq!(res_account2.id, account_id2);
            assert_eq!(res_account2.debits_posted, 0);
            assert_eq!(res_account2.credits_posted, 10);
        }

        {
            let result = client.lookup_transfers(&[transfer_id1]).await?;

            assert_eq!(result.len(), 1);
            let res_transfer1 = result[0].unwrap();

            assert_eq!(res_transfer1.id, transfer_id1);
            assert_eq!(res_transfer1.debit_account_id, account_id1);
            assert_eq!(res_transfer1.credit_account_id, account_id2);
            assert_eq!(res_transfer1.amount, 10);
        }

        {
            let result = client
                .get_account_transfers(tb::AccountFilter {
                    account_id: account_id1,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    code: TEST_CODE,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::AccountFilterFlags::Credits | tb::AccountFilterFlags::Debits,
                })
                .await?;

            assert_eq!(result.len(), 1);

            let res_transfer = &result[0];

            assert_eq!(res_transfer.id, transfer_id1);
            assert_eq!(res_transfer.debit_account_id, account_id1);
            assert_eq!(res_transfer.credit_account_id, account_id2);
            assert_eq!(res_transfer.amount, 10);
        }

        {
            let result = client
                .get_account_balances(tb::AccountFilter {
                    account_id: account_id1,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    code: TEST_CODE,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::AccountFilterFlags::Credits | tb::AccountFilterFlags::Debits,
                })
                .await?;

            assert_eq!(result.len(), 1);

            let res_balance_1 = &result[0];

            assert_eq!(res_balance_1.debits_posted, 10);
            assert_eq!(res_balance_1.credits_posted, 0);
        }

        {
            let result = client
                .query_accounts(tb::QueryFilter {
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: account_id2_user_data_32,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::QueryFilterFlags::default(),
                })
                .await?;

            assert_eq!(result.len(), 1);

            let res_account = &result[0];

            assert_eq!(res_account.id, account_id2);
        }

        {
            let result = client
                .query_transfers(tb::QueryFilter {
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: transfer_id1_user_data_32,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::QueryFilterFlags::default(),
                })
                .await?;

            assert_eq!(result.len(), 1);

            let res_transfer = &result[0];

            assert_eq!(res_transfer.id, transfer_id1);
        }

        Ok(())
    })
}

#[test]
fn ctor_fail() -> anyhow::Result<()> {
    let harness = TestHarness::new("smoke")?;
    harness.prepare_database()?;

    let client = tb::Client::new(0, "hey");

    assert!(matches!(client, Err(tb::InitStatus::AddressInvalid)));

    Ok(())
}

#[test]
fn dtor() -> anyhow::Result<()> {
    let (_harness, client) = server_and_client()?;

    block_on(async {
        // Let's at least talk to the server before dropping
        let _ = client.create_accounts(&[]).await?;
        drop(client);
        Ok(())
    })
}

#[test]
fn close() -> anyhow::Result<()> {
    let (_harness, client) = server_and_client()?;

    block_on(async {
        // Let's at least talk to the server before dropping
        let _ = client.create_accounts(&[]).await?;
        client.close().await;
        Ok(())
    })
}

#[test]
fn too_many_events() -> anyhow::Result<()> {
    let (_harness, client) = server_and_client()?;

    block_on(async {
        let accounts = lots_of_accounts();
        let result = client.create_accounts(&accounts).await;

        assert_eq!(result, Err(tb::PacketStatus::TooMuchData));

        Ok(())
    })
}

fn lots_of_accounts() -> Vec<tb::Account> {
    let mut accounts = vec![];
    let num_accounts = 10_000;
    for i in 0..num_accounts {
        let account = tb::Account {
            id: i + 1,
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            reserved: tb::Reserved::default(),
            ledger: TEST_LEDGER,
            code: TEST_CODE,
            flags: tb::AccountFlags::History,
            timestamp: 0,
        };
        accounts.push(account);
    }
    return accounts;
}

#[test]
fn zero_events() -> anyhow::Result<()> {
    let (_harness, client) = server_and_client()?;

    block_on(async {
        let result = client.create_accounts(&[]).await?;

        assert!(result.is_empty());

        Ok(())
    })
}

// Test that the `collect_results` helper correctly
// correlates (possibly-empty) results with the request array.
#[test]
fn result_correlation() -> anyhow::Result<()> {
    let (_harness, client) = server_and_client()?;

    block_on(async {
        let result = client
            .create_accounts(&[
                tb::Account {
                    id: 1,
                    debits_pending: 0,
                    debits_posted: 0,
                    credits_pending: 0,
                    credits_posted: 0,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    reserved: tb::Reserved::default(),
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    flags: tb::AccountFlags::History,
                    timestamp: 0,
                },
                tb::Account {
                    id: 2,
                    debits_pending: 0,
                    debits_posted: 0,
                    credits_pending: 0,
                    credits_posted: 0,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    reserved: tb::Reserved::default(),
                    ledger: 0, // err
                    code: TEST_CODE,
                    flags: tb::AccountFlags::History,
                    timestamp: 0,
                },
                tb::Account {
                    id: 3,
                    debits_pending: 0,
                    debits_posted: 0,
                    credits_pending: 0,
                    credits_posted: 0,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    reserved: tb::Reserved::default(),
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    flags: tb::AccountFlags::History,
                    timestamp: 0,
                },
                tb::Account {
                    id: 4,
                    debits_pending: 0,
                    debits_posted: 0,
                    credits_pending: 0,
                    credits_posted: 0,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    reserved: tb::Reserved::default(),
                    ledger: TEST_LEDGER,
                    code: 0, // err
                    flags: tb::AccountFlags::History,
                    timestamp: 0,
                },
                tb::Account {
                    id: 5,
                    debits_pending: 0,
                    debits_posted: 0,
                    credits_pending: 0,
                    credits_posted: 0,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    reserved: tb::Reserved::default(),
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    flags: tb::AccountFlags::History,
                    timestamp: 0,
                },
            ])
            .await?;

        assert_eq!(result.len(), 5);

        assert_eq!(result[0], tb::CreateAccountResult::Ok);
        assert_eq!(result[1], tb::CreateAccountResult::LedgerMustNotBeZero);
        assert_eq!(result[2], tb::CreateAccountResult::Ok);
        assert_eq!(result[3], tb::CreateAccountResult::CodeMustNotBeZero);
        assert_eq!(result[4], tb::CreateAccountResult::Ok);

        Ok(())
    })
}

#[test]
fn multithread() -> anyhow::Result<()> {
    let (_harness, client) = server_and_client()?;
    let client = Arc::new(client);

    let num_threads = 16;
    let num_requests = 1_000;

    let barrier = Arc::new(Barrier::new(num_threads));

    let join_handles = std::iter::repeat(()).take(num_threads).map(|_| {
        let client = client.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || -> anyhow::Result<()> {
            barrier.wait();
            block_on(async {
                for _ in 0..num_requests {
                    let results = client
                        .create_accounts(&[tb::Account {
                            id: random_u128(),
                            debits_pending: 0,
                            debits_posted: 0,
                            credits_pending: 0,
                            credits_posted: 0,
                            user_data_128: 0,
                            user_data_64: 0,
                            user_data_32: 0,
                            reserved: tb::Reserved::default(),
                            ledger: TEST_LEDGER,
                            code: TEST_CODE,
                            flags: tb::AccountFlags::History,
                            timestamp: 0,
                        }])
                        .await?;

                    for result in results {
                        assert_eq!(result, tb::CreateAccountResult::Ok);
                    }
                }

                Ok(())
            })
        })
    });

    // collect the handles to evaluate the thread::spawns
    let join_handles = join_handles.collect::<Vec<_>>();

    for join_handle in join_handles {
        let res = join_handle.join().expect("no panic");
        assert!(!res.is_err());
    }

    block_on(async {
        let client = Arc::into_inner(client).expect("arc");

        client.close().await;

        Ok(())
    })
}

#[test]
fn concurrent_requests() -> anyhow::Result<()> {
    let (_harness, client) = server_and_client()?;

    let mut responses = Vec::new();

    for _ in 0..10 {
        let response = client.create_accounts(&[tb::Account {
            id: random_u128(),
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            reserved: tb::Reserved::default(),
            ledger: TEST_LEDGER,
            code: TEST_CODE,
            flags: tb::AccountFlags::History,
            timestamp: 0,
        }]);
        responses.push(response);
    }

    for response in responses {
        let response = block_on(async { response.await })?;
        for result in response {
            assert_eq!(result, tb::CreateAccountResult::Ok);
        }
    }

    Ok(())
}
