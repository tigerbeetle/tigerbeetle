use std::cell::UnsafeCell;
use std::env;
use std::env::consts::EXE_SUFFIX;
use std::io::{BufRead as _, BufReader};
use std::mem;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier, Mutex, MutexGuard, Once};

use futures::executor::block_on;
use futures::pin_mut;
use futures::{Stream, StreamExt};

use tigerbeetle as tb;

// Singleton test database.
// This can be a OnceLock in Rust 1.70+, and LazyLock in 1.80.
fn get_test_db() -> &'static TestDb {
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

struct TestDb {
    port: u16,
    // Keep the server's stdin handle open as long as the test process is running,
    // at which point the server will terminate.
    _server: Child,
}

fn tigerbeetle_bin() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../../../tigerbeetle{EXE_SUFFIX}")
}

fn work_dir() -> &'static str {
    env!("CARGO_TARGET_TMPDIR")
}

impl TestDb {
    fn new() -> anyhow::Result<TestDb> {
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

        let server = Self::start(&["--addresses=0", "--cache-grid=128MiB", database_name])?;

        Ok(server)
    }

    /// Create a unique development-mode server for a specific test.
    ///
    /// Returns a `DevelopmentDb` that holds a mutex guard, ensuring only one
    /// development-mode server runs at a time.
    fn new_development(label: &str) -> anyhow::Result<DevelopmentDb> {
        let guard = DEVELOPMENT_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

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

        let db = Self::start(&["--addresses=0", "--development", &database_name])?;

        Ok(DevelopmentDb { db, _guard: guard })
    }

    fn start(args: &[&str]) -> anyhow::Result<TestDb> {
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

    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

/// Only one development-mode server can run at a time to avoid overloading the machine.
static DEVELOPMENT_MUTEX: Mutex<()> = Mutex::new(());

struct DevelopmentDb {
    db: TestDb,
    _guard: MutexGuard<'static, ()>,
}

impl DevelopmentDb {
    fn address(&self) -> String {
        self.db.address()
    }
}

fn test_client() -> anyhow::Result<tb::Client> {
    let client = tb::Client::new(0, &get_test_db().address())?;
    Ok(client)
}

fn assert_send<T: Send>(t: T) -> T {
    t
}

const TEST_LEDGER: u32 = 10;
const TEST_CODE: u16 = 20;

#[test]
fn smoke() -> anyhow::Result<()> {
    let account_id1 = tb::id();
    let account_id2 = tb::id();
    let transfer_id1 = tb::id();

    let account_id2_user_data_128 = tb::id();
    let transfer_id1_user_data_128 = tb::id();

    block_on(async {
        let client = test_client()?;

        {
            let fut = client.create_accounts(&[
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
                    user_data_128: account_id2_user_data_128,
                    user_data_64: 0,
                    user_data_32: 0,
                    reserved: tb::Reserved::default(),
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    flags: tb::AccountFlags::History,
                    timestamp: 0,
                },
            ]);
            let result = assert_send(fut).await?;

            assert_eq!(result.len(), 0);
        }

        {
            let result = client
                .create_transfers(&[tb::Transfer {
                    id: transfer_id1,
                    debit_account_id: account_id1,
                    credit_account_id: account_id2,
                    amount: 10,
                    pending_id: 0,
                    user_data_128: transfer_id1_user_data_128,
                    user_data_64: 0,
                    user_data_32: 0,
                    timeout: 0,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    flags: tb::TransferFlags::default(),
                    timestamp: 0,
                }])
                .await?;

            assert_eq!(result.len(), 0);
        }

        {
            let result = client.lookup_accounts(&[account_id1, account_id2]).await?;

            assert_eq!(result.len(), 2);
            let res_account1 = result[0];
            let res_account2 = result[1];

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
            let res_transfer1 = result[0];

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
                    user_data_128: account_id2_user_data_128,
                    user_data_64: 0,
                    user_data_32: 0,
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
                    user_data_128: transfer_id1_user_data_128,
                    user_data_64: 0,
                    user_data_32: 0,
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
    let client = tb::Client::new(0, "hey");

    assert!(matches!(client, Err(tb::InitStatus::AddressInvalid)));

    Ok(())
}

#[test]
fn dtor() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        // Let's at least talk to the server before dropping
        let _ = client.create_accounts(&[]).await?;
        drop(client);
        Ok(())
    })
}

#[test]
fn close() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        let _ = client.create_accounts(&[]).await?;
        client.close().await;
        Ok(())
    })
}

// Send a request and immediately drop the client.
// Should still clean up correctly.
#[test]
fn dtor_no_wait() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        let _ = client.create_accounts(&[]);
        drop(client);
        Ok(())
    })
}

#[test]
fn close_no_wait() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        let _ = client.create_accounts(&[]);
        let _ = client.close();
        Ok(())
    })
}

#[test]
fn client_drop_before_future_awaited() -> anyhow::Result<()> {
    let future = {
        let client = test_client()?;

        let account = tb::Account {
            id: tb::id(),
            ledger: TEST_LEDGER,
            code: TEST_CODE,
            ..Default::default()
        };

        let future = client.create_accounts(&[account]);
        drop(client);
        future
    };

    let result = block_on(async { future.await });

    match result {
        Ok(_) => {}
        Err(tb::PacketStatus::ClientShutdown) => {}
        Err(_) => panic!(),
    }

    Ok(())
}

#[test]
fn client_drop_causes_shutdown_status() -> anyhow::Result<()> {
    let futures = {
        let client = test_client()?;

        let mut futures = Vec::new();
        for _ in 0..10 {
            let account = tb::Account {
                id: tb::id(),
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            };
            futures.push(client.create_accounts(&[account]));
        }

        drop(client);
        futures
    };

    let mut shutdown_count = 0;

    for future in futures {
        match block_on(async { future.await }) {
            Ok(_) => {}
            Err(tb::PacketStatus::ClientShutdown) => shutdown_count += 1,
            Err(_) => panic!(),
        }
    }

    assert!(shutdown_count > 0);

    Ok(())
}

#[test]
fn too_many_events() -> anyhow::Result<()> {
    let client = test_client()?;

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
    for _ in 0..num_accounts {
        let account = tb::Account {
            id: tb::id(),
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
fn zero_events_create_accounts() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        let result = client.create_accounts(&[]).await?;

        assert!(result.is_empty());

        Ok(())
    })
}

#[test]
fn zero_events_create_transfers() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        let result = client.create_transfers(&[]).await?;

        assert!(result.is_empty());

        Ok(())
    })
}

#[test]
fn zero_events_lookup_accounts() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        let result = client.lookup_accounts(&[]).await?;

        assert!(result.is_empty());

        Ok(())
    })
}

#[test]
fn zero_events_lookup_transfers() -> anyhow::Result<()> {
    let client = test_client()?;

    block_on(async {
        let result = client.lookup_transfers(&[]).await?;

        assert!(result.is_empty());

        Ok(())
    })
}

#[test]
fn multithread() -> anyhow::Result<()> {
    let client = test_client()?;
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
                            id: tb::id(),
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

                    assert_eq!(results.len(), 0);
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
        let client = Arc::try_unwrap(client).expect("arc");

        client.close().await;

        Ok(())
    })
}

#[test]
fn concurrent_requests() -> anyhow::Result<()> {
    let client = test_client()?;

    let mut responses = Vec::new();

    for _ in 0..10 {
        let response = client.create_accounts(&[tb::Account {
            id: tb::id(),
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
        assert_eq!(response.len(), 0);
    }

    Ok(())
}

// A potentially suprising behavior, documented in the crate docs.
#[test]
fn client_drop_loses_pending_transactions() -> anyhow::Result<()> {
    let mut ids = Vec::new();

    // Queue up lots of transactions, drop their futures, drop the client.
    {
        let client = test_client()?;

        // Timing-sensitive - trying to create enough pending transactions that
        // not all will be completed. I think test is unlikely to fail because
        // of timing problems since it takes quite some time to process a
        // transaction. Locally setting this to 1 still fails.
        let transaction_count = 100_000;
        for _ in 0..transaction_count {
            let id = tb::id();
            let _ = client.create_accounts(&[tb::Account {
                id,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            }]);
            ids.push(id);
        }
    }

    // Some of those transactions will have been dropped by tb_client.
    let client = test_client()?;

    // Reverse because later transactions most likely to be lost.
    ids.reverse();

    for next_ids in ids.chunks(8189) {
        let results = block_on(client.lookup_accounts(next_ids))?;
        if results.len() < next_ids.len() {
            // This is what we expect.
            return Ok(());
        }
    }

    panic!("unexpected all transactions succeeded");
}

/// Query multiple transfers for a single account, with paging.
///
/// This handles the case where there are too many results to fit into
/// a single batch, by making multiple sequential queries, incrementing
/// the timestamp range (or decrementing for reverse queries).
///
/// The [`AccountFilter`]'s `limit` field should be set to greater than 1
/// to set the page size. `limit` must be less than or equal to the build-time
/// configuration of the TigerBeetle server's batch size (default 8189).
///
/// To perform a reverse query set [`AccountFilterFlag::Reversed`].
//
// NB: This is a runnable version of an example in the crate docs.
// Try to keep them in sync.
fn get_account_transfers_paged(
    client: &tb::Client,
    event: tb::AccountFilter,
) -> impl Stream<Item = Result<Vec<tb::Transfer>, tb::PacketStatus>> + '_ {
    assert!(
        event.limit > 1,
        "paged queries should use an explicit limit"
    );

    enum State {
        Start,
        Continue(u64),
        End,
    }

    let is_reverse = event.flags.contains(tb::AccountFilterFlags::Reversed);

    futures::stream::unfold(State::Start, move |state| async move {
        let event = match state {
            State::Start => event,
            State::Continue(timestamp_begin) => {
                if !is_reverse {
                    tb::AccountFilter {
                        timestamp_min: timestamp_begin,
                        ..event
                    }
                } else {
                    tb::AccountFilter {
                        timestamp_max: timestamp_begin,
                        ..event
                    }
                }
            }
            State::End => return None,
        };
        let result_next = client.get_account_transfers(event).await;
        match result_next {
            Ok(result_next) => {
                let result_len = u32::try_from(result_next.len()).expect("u32");
                let must_page = result_len == event.limit;
                if must_page {
                    let timestamp_first = result_next.first().expect("item").timestamp;
                    let timestamp_last = result_next.last().expect("item").timestamp;
                    let (timestamp_begin_next, should_continue) = if !is_reverse {
                        assert!(timestamp_first < timestamp_last);
                        let timestamp_begin_next = timestamp_last.checked_add(1).expect("overflow");
                        assert_ne!(timestamp_begin_next, u64::MAX);
                        let should_continue =
                            timestamp_begin_next <= event.timestamp_max || event.timestamp_max == 0;
                        (timestamp_begin_next, should_continue)
                    } else {
                        assert!(timestamp_first > timestamp_last);
                        let timestamp_begin_next = timestamp_last.checked_sub(1).expect("overflow");
                        assert_ne!(timestamp_begin_next, 0);
                        let should_continue =
                            timestamp_begin_next >= event.timestamp_min || event.timestamp_min == 0;
                        (timestamp_begin_next, should_continue)
                    };
                    if should_continue {
                        Some((Ok(result_next), State::Continue(timestamp_begin_next)))
                    } else {
                        Some((Ok(result_next), State::End))
                    }
                } else {
                    Some((Ok(result_next), State::End))
                }
            }
            Err(result_next) => Some((Err(result_next), State::End)),
        }
    })
}

struct PagingTestParams {
    account_id1: u128,
    #[allow(unused)]
    account_id2: u128,
    transfer_count: usize,
}

fn make_paging_test_transfers(client: &tb::Client) -> anyhow::Result<PagingTestParams> {
    let batch_size: usize = 1234;
    let transfer_count: usize = 5678;
    let account_id1 = tb::id();
    let account_id2 = tb::id();

    let account1 = tb::Account {
        id: account_id1,
        ledger: TEST_LEDGER,
        code: TEST_CODE,
        ..Default::default()
    };
    let account2 = tb::Account {
        id: account_id2,
        ledger: TEST_LEDGER,
        code: TEST_CODE,
        ..Default::default()
    };

    let transfers: Vec<_> = std::iter::from_fn(|| {
        Some(tb::Transfer {
            id: tb::id(),
            debit_account_id: account_id1,
            credit_account_id: account_id2,
            amount: 100,
            ledger: TEST_LEDGER,
            code: TEST_CODE,
            ..Default::default()
        })
    })
    .take(transfer_count)
    .collect();

    block_on(async {
        let account_results = client.create_accounts(&[account1, account2]).await?;
        assert_eq!(account_results.len(), 0);

        for transfers in transfers.chunks(batch_size) {
            let transfer_results = client.create_transfers(transfers).await?;
            assert_eq!(transfer_results.len(), 0);
        }

        Ok(PagingTestParams {
            account_id1,
            account_id2,
            transfer_count,
        })
    })
}

#[test]
fn paging_forward() -> anyhow::Result<()> {
    let client = test_client()?;
    let test_params = make_paging_test_transfers(&client)?;

    let query_results = get_account_transfers_paged(
        &client,
        tb::AccountFilter {
            account_id: test_params.account_id1,
            limit: 1000,
            flags: tb::AccountFilterFlags::Debits,
            ..Default::default()
        },
    );

    pin_mut!(query_results);

    let mut batches = 0;
    let mut transfer_count = 0;

    while let Some(query_results) = block_on(query_results.next()) {
        let query_results = query_results?;
        batches += 1;
        transfer_count += query_results.len();
    }

    assert!(batches > 1);
    assert_eq!(transfer_count, test_params.transfer_count);

    Ok(())
}

#[test]
fn paging_reverse() -> anyhow::Result<()> {
    let client = test_client()?;
    let test_params = make_paging_test_transfers(&client)?;

    let query_results = get_account_transfers_paged(
        &client,
        tb::AccountFilter {
            account_id: test_params.account_id1,
            limit: 1000,
            flags: tb::AccountFilterFlags::Debits | tb::AccountFilterFlags::Reversed,
            ..Default::default()
        },
    );

    pin_mut!(query_results);

    let mut batches = 0;
    let mut transfer_count = 0;

    while let Some(query_results) = block_on(query_results.next()) {
        let query_results = query_results?;
        batches += 1;
        transfer_count += query_results.len();
    }

    assert!(batches > 1);
    assert_eq!(transfer_count, test_params.transfer_count);

    Ok(())
}

// NB: This is a runnable version of an example in the `create_accounts` docs.
// Try to keep them in sync.
#[test]
fn example_create_accounts() -> Result<(), Box<dyn std::error::Error>> {
    async fn make_create_accounts_request(
        client: &tb::Client,
        accounts: &[tb::Account],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let create_accounts_results = client.create_accounts(accounts).await?;
        let create_accounts_results_merged =
            merge_create_accounts_results(accounts, create_accounts_results);
        for (account, create_account_result) in create_accounts_results_merged {
            match create_account_result {
                tb::CreateAccountResult::Ok | tb::CreateAccountResult::Exists => {
                    handle_create_account_success(account, create_account_result).await?;
                }
                _ => {
                    handle_create_account_failure(account, create_account_result).await?;
                }
            }
        }
        Ok(())
    }

    /// An iterator over both successful and unsuccessful `create_account` results.
    fn merge_create_accounts_results(
        accounts: &[tb::Account],
        results: Vec<tb::CreateAccountsResult>,
    ) -> impl Iterator<Item = (&tb::Account, tb::CreateAccountResult)> + '_ {
        let mut results = results.into_iter().peekable();
        accounts
            .iter()
            .enumerate()
            .map(move |(i, account)| match results.peek().copied() {
                Some(result) if result.index == i => {
                    let _ = results.next();
                    (account, result.result)
                }
                _ => (account, tb::CreateAccountResult::Ok),
            })
    }

    async fn handle_create_account_success(
        _account: &tb::Account,
        _result: tb::CreateAccountResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn handle_create_account_failure(
        _account: &tb::Account,
        _result: tb::CreateAccountResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    block_on(async {
        let gen_accounts = || {
            let duplicate_id = tb::id();
            [
                tb::Account {
                    id: duplicate_id,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    ..Default::default()
                },
                tb::Account {
                    id: duplicate_id,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    ..Default::default()
                },
                tb::Account {
                    id: tb::id(),
                    ledger: TEST_LEDGER,
                    code: 0,
                    ..Default::default()
                },
            ]
        };
        let results_expected = &[
            tb::CreateAccountsResult {
                index: 1,
                result: tb::CreateAccountResult::Exists,
            },
            tb::CreateAccountsResult {
                index: 2,
                result: tb::CreateAccountResult::CodeMustNotBeZero,
            },
        ];
        let merged_results_expected = &[
            tb::CreateAccountResult::Ok,
            tb::CreateAccountResult::Exists,
            tb::CreateAccountResult::CodeMustNotBeZero,
        ];

        let client = test_client()?;

        // Test the example.
        make_create_accounts_request(&client, &gen_accounts()).await?;

        // Also test that the results are what we expect.
        let results_actual = client.create_accounts(&gen_accounts()).await?;
        assert_eq!(results_expected, &results_actual[..]);

        // Test `merge_create_accounts_results`
        let accounts = gen_accounts();
        let results_actual = client.create_accounts(&accounts).await?;
        let results_merged: Vec<_> =
            merge_create_accounts_results(&accounts, results_actual).collect();
        let results_expected: Vec<(&tb::Account, tb::CreateAccountResult)> = accounts
            .iter()
            .zip(merged_results_expected)
            .map(|(account, create_account_result)| (account, *create_account_result))
            .collect();
        assert_eq!(results_expected, results_merged);

        Ok(())
    })
}

// NB: This is a runnable version of an example in the `create_transfers` docs.
// Try to keep them in sync.
#[test]
fn example_create_transfers() -> Result<(), Box<dyn std::error::Error>> {
    async fn make_create_transfers_request(
        client: &tb::Client,
        transfers: &[tb::Transfer],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let create_transfers_results = client.create_transfers(transfers).await?;
        let create_transfers_results_merged =
            merge_create_transfers_results(transfers, create_transfers_results);
        for (transfer, create_transfer_result) in create_transfers_results_merged {
            match create_transfer_result {
                tb::CreateTransferResult::Ok | tb::CreateTransferResult::Exists => {
                    handle_create_transfer_success(transfer, create_transfer_result).await?;
                }
                _ => {
                    handle_create_transfer_failure(transfer, create_transfer_result).await?;
                }
            }
        }
        Ok(())
    }

    fn merge_create_transfers_results(
        transfers: &[tb::Transfer],
        results: Vec<tb::CreateTransfersResult>,
    ) -> impl Iterator<Item = (&tb::Transfer, tb::CreateTransferResult)> + '_ {
        let mut results = results.into_iter().peekable();
        transfers
            .iter()
            .enumerate()
            .map(move |(i, transfer)| match results.peek().copied() {
                Some(result) if result.index == i => {
                    let _ = results.next();
                    (transfer, result.result)
                }
                _ => (transfer, tb::CreateTransferResult::Ok),
            })
    }

    async fn handle_create_transfer_success(
        _transfer: &tb::Transfer,
        _result: tb::CreateTransferResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn handle_create_transfer_failure(
        _transfer: &tb::Transfer,
        _result: tb::CreateTransferResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    block_on(async {
        let account_id1 = tb::id();
        let account_id2 = tb::id();
        let client = test_client()?;

        let accounts = [
            tb::Account {
                id: account_id1,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
            tb::Account {
                id: account_id2,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
        ];
        client.create_accounts(&accounts).await?;

        let gen_transfers = || {
            let duplicate_id = tb::id();
            [
                tb::Transfer {
                    id: duplicate_id,
                    debit_account_id: account_id1,
                    credit_account_id: account_id2,
                    amount: 100,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    ..Default::default()
                },
                tb::Transfer {
                    id: duplicate_id,
                    debit_account_id: account_id1,
                    credit_account_id: account_id2,
                    amount: 100,
                    ledger: TEST_LEDGER,
                    code: TEST_CODE,
                    ..Default::default()
                },
                tb::Transfer {
                    id: tb::id(),
                    debit_account_id: account_id1,
                    credit_account_id: account_id2,
                    amount: 100,
                    ledger: TEST_LEDGER,
                    code: 0,
                    ..Default::default()
                },
            ]
        };

        let results_expected = &[
            tb::CreateTransfersResult {
                index: 1,
                result: tb::CreateTransferResult::Exists,
            },
            tb::CreateTransfersResult {
                index: 2,
                result: tb::CreateTransferResult::CodeMustNotBeZero,
            },
        ];

        // Test the example.
        make_create_transfers_request(&client, &gen_transfers()).await?;

        // Also test that the results are what we expect.
        let results_actual = client.create_transfers(&gen_transfers()).await?;
        assert_eq!(results_expected, &results_actual[..]);

        Ok(())
    })
}

// NB: This is a runnable version of an example in the `lookup_accounts` docs.
// Try to keep them in sync.
#[test]
fn example_lookup_accounts() -> Result<(), Box<dyn std::error::Error>> {
    async fn make_lookup_accounts_request(
        client: &tb::Client,
        accounts: &[u128],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let lookup_accounts_results = client.lookup_accounts(accounts).await?;
        let lookup_accounts_results_merged =
            merge_lookup_accounts_results(accounts, lookup_accounts_results);
        for (account_id, maybe_account) in lookup_accounts_results_merged {
            match maybe_account {
                Some(account) => {
                    handle_lookup_accounts_success(account).await?;
                }
                None => {
                    handle_lookup_accounts_failure(account_id).await?;
                }
            }
        }
        Ok(())
    }

    /// An iterator over both successful and unsuccessful lookup results.
    fn merge_lookup_accounts_results(
        accounts: &[u128],
        results: Vec<tb::Account>,
    ) -> impl Iterator<Item = (u128, Option<tb::Account>)> + '_ {
        let mut results = results.into_iter().peekable();
        accounts.iter().map(move |&id| match results.peek() {
            Some(acc) if acc.id == id => (id, results.next()),
            _ => (id, None),
        })
    }

    async fn handle_lookup_accounts_success(
        _account: tb::Account,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn handle_lookup_accounts_failure(
        _account_id: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    block_on(async {
        let account_id1 = tb::id();
        let account_id2 = tb::id();
        let accounts = &[
            tb::Account {
                id: account_id1,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
            tb::Account {
                id: account_id2,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
        ];
        let account_bogus1 = tb::id();
        let account_bogus2 = tb::id();
        let account_bogus3 = tb::id();
        let accounts_lookup = &[
            account_bogus1,
            account_id1,
            account_bogus2,
            account_id2,
            account_bogus3,
        ];
        let results_expected = &[accounts[0], accounts[1]];
        let merge_expected = &[
            (account_bogus1, None),
            (account_id1, Some(accounts[0])),
            (account_bogus2, None),
            (account_id2, Some(accounts[1])),
            (account_bogus3, None),
        ];

        let client = test_client()?;

        let _ = client.create_accounts(accounts).await?;

        // Test the example.
        make_lookup_accounts_request(&client, accounts_lookup).await?;

        // Also test that the results are what we expect.
        let results_actual = client.lookup_accounts(accounts_lookup).await?;
        let results_actual: Vec<_> = results_actual
            .into_iter()
            .map(|account| tb::Account {
                timestamp: Default::default(),
                ..account
            })
            .collect();
        assert_eq!(results_expected, &results_actual[..]);

        // Test the `merge_lookup_accounts_results` function.
        let merge_actual: Vec<_> =
            merge_lookup_accounts_results(accounts_lookup, results_actual).collect();
        assert_eq!(merge_expected, &merge_actual[..]);

        Ok(())
    })
}

// NB: This is a runnable version of an example in the `lookup_transfers` docs.
// Try to keep them in sync.
#[test]
fn example_lookup_transfers() -> Result<(), Box<dyn std::error::Error>> {
    async fn make_lookup_transfers_request(
        client: &tb::Client,
        transfers: &[u128],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let lookup_transfers_results = client.lookup_transfers(transfers).await?;
        let lookup_transfers_results_merged =
            merge_lookup_transfers_results(transfers, lookup_transfers_results);
        for (transfer_id, maybe_transfer) in lookup_transfers_results_merged {
            match maybe_transfer {
                Some(transfer) => {
                    handle_lookup_transfers_success(transfer).await?;
                }
                None => {
                    handle_lookup_transfers_failure(transfer_id).await?;
                }
            }
        }
        Ok(())
    }

    /// An iterator over both successful and unsuccessful lookup results.
    fn merge_lookup_transfers_results(
        transfers: &[u128],
        results: Vec<tb::Transfer>,
    ) -> impl Iterator<Item = (u128, Option<tb::Transfer>)> + '_ {
        let mut results = results.into_iter().peekable();
        transfers.iter().map(move |&id| match results.peek() {
            Some(transfer) if transfer.id == id => (id, results.next()),
            _ => (id, None),
        })
    }

    async fn handle_lookup_transfers_success(
        _transfer: tb::Transfer,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn handle_lookup_transfers_failure(
        _transfer_id: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    block_on(async {
        let account_id1 = tb::id();
        let account_id2 = tb::id();
        let accounts = &[
            tb::Account {
                id: account_id1,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
            tb::Account {
                id: account_id2,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
        ];
        let transfer_id1 = tb::id();
        let transfer_id2 = tb::id();
        let transfers = &[
            tb::Transfer {
                id: transfer_id1,
                debit_account_id: account_id1,
                credit_account_id: account_id2,
                amount: 100,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
            tb::Transfer {
                id: transfer_id2,
                debit_account_id: account_id2,
                credit_account_id: account_id1,
                amount: 50,
                ledger: TEST_LEDGER,
                code: TEST_CODE,
                ..Default::default()
            },
        ];
        let transfer_bogus1 = tb::id();
        let transfer_bogus2 = tb::id();
        let transfer_bogus3 = tb::id();
        let transfers_lookup = &[
            transfer_bogus1,
            transfer_id1,
            transfer_bogus2,
            transfer_id2,
            transfer_bogus3,
        ];
        let results_expected = &[transfers[0], transfers[1]];
        let merge_expected = &[
            (transfer_bogus1, None),
            (transfer_id1, Some(transfers[0])),
            (transfer_bogus2, None),
            (transfer_id2, Some(transfers[1])),
            (transfer_bogus3, None),
        ];

        let client = test_client()?;

        let _ = client.create_accounts(accounts).await?;
        let _ = client.create_transfers(transfers).await?;

        // Test the example.
        make_lookup_transfers_request(&client, transfers_lookup).await?;

        // Also test that the results are what we expect.
        let results_actual = client.lookup_transfers(transfers_lookup).await?;
        let results_actual: Vec<_> = results_actual
            .into_iter()
            .map(|transfer| tb::Transfer {
                timestamp: Default::default(),
                ..transfer
            })
            .collect();
        assert_eq!(results_expected, &results_actual[..]);

        // Test the `merge_lookup_transfers_results` function.
        let merge_actual: Vec<_> =
            merge_lookup_transfers_results(transfers_lookup, results_actual).collect();
        assert_eq!(merge_expected, &merge_actual[..]);

        Ok(())
    })
}

// This is a copy of the Java testClientEvicted case.
#[test]
fn client_evicted() -> anyhow::Result<()> {
    const CLIENTS_MAX: usize = 64;

    // Use a separate server to avoid evicting the shared test client.
    let server = TestDb::new_development("client_evicted")?;
    let address = server.address();

    let client_evict = tb::Client::new(0, &address)?;

    let accounts = block_on(client_evict.lookup_accounts(&[tb::id()]))?;
    assert_eq!(accounts.len(), 0);

    let mut handles = Vec::new();
    for _ in 0..CLIENTS_MAX {
        let address = address.clone();
        handles.push(std::thread::spawn(move || {
            let client = tb::Client::new(0, &address).unwrap();
            let accounts = block_on(client.lookup_accounts(&[tb::id()])).unwrap();
            assert_eq!(accounts.len(), 0);
        }));
    }

    for handle in handles {
        let _ = handle.join();
    }

    // The original client should now be evicted.
    let result = block_on(client_evict.lookup_accounts(&[tb::id()]));
    assert_eq!(result, Err(tb::PacketStatus::ClientEvicted));

    Ok(())
}

#[test]
fn many_clients_no_deadlock() -> anyhow::Result<()> {
    const CLIENTS_MAX: usize = 65;

    block_on(async {
        // Use a separate server to avoid evicting the shared test client.
        let server = TestDb::new_development("many_clients_no_deadlock")?;
        let address = server.address();

        let mut clients = Vec::new();

        for _ in 0..CLIENTS_MAX {
            let client = tb::Client::new(0, &address)?;
            clients.push(client);
        }

        use futures::stream::FuturesUnordered;
        use futures::StreamExt;

        let mut lookup_futures: FuturesUnordered<_> = clients
            .iter()
            .map(|client| client.lookup_accounts(&[tb::id()]))
            .collect();

        let mut i = 0;
        while let Some(lookup) = lookup_futures.next().await {
            eprintln!("awaiting {i}");
            i += 1;
            match lookup {
                Ok(accounts) => assert_eq!(accounts.len(), 0),
                Err(tb::PacketStatus::ClientEvicted) => {
                    eprintln!("evicted");
                }
                Err(e) => panic!("unexpected error {e}"),
            }
        }

        for client in clients {
            let _ = client;
        }

        Ok(())
    })
}

#[test]
fn eviction_stress_1() -> anyhow::Result<()> {
    const CLIENTS: usize = 65;
    const THREADS_PER_CLIENT: usize = 4;
    const REQUESTS_PER_THREAD: usize = 4;
    // Many threads, lets try to minimize ram.
    const STACK_SIZE: usize = 1024 * 64;

    let server = TestDb::new_development("eviction_stress_1")?;
    let address = server.address();

    let barrier = Arc::new(Barrier::new(CLIENTS * THREADS_PER_CLIENT));

    let clients: Vec<Arc<tb::Client>> = (0..CLIENTS)
        .map(|_| Ok(Arc::new(tb::Client::new(0, &address)?)))
        .collect::<anyhow::Result<_>>()?;

    let thread_clients = clients
        .iter()
        .flat_map(|c| std::iter::repeat(c.clone()).take(THREADS_PER_CLIENT));

    struct ThreadInput(Arc<tb::Client>, Arc<Barrier>);
    struct ThreadResult(Vec<Result<Vec<tb::Account>, tb::PacketStatus>>);

    #[derive(Default)]
    struct Results {
        successes: usize,
        evictions: usize,
        failures: usize,
    }

    let threads: Vec<std::thread::JoinHandle<ThreadResult>> = thread_clients
        .into_iter()
        .map(|client| {
            let barrier = barrier.clone();
            Ok(std::thread::Builder::new()
                .stack_size(STACK_SIZE)
                .spawn(|| run_thread(ThreadInput(client, barrier)))?)
        })
        .collect::<anyhow::Result<_>>()?;

    fn run_thread(ThreadInput(client, barrier): ThreadInput) -> ThreadResult {
        block_on(async {
            barrier.wait();

            let mut results = Vec::new();

            for _ in 0..REQUESTS_PER_THREAD {
                results.push(client.lookup_accounts(&[tb::id()]).await);
                std::thread::yield_now();
            }

            ThreadResult(results)
        })
    }

    let thread_results: Vec<ThreadResult> = threads
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("thread panicked"))
        })
        .collect::<anyhow::Result<_>>()?;

    assert_eq!(thread_results.len(), CLIENTS * THREADS_PER_CLIENT);

    let results = thread_results
        .iter()
        .flat_map(|ThreadResult(results)| results)
        .fold(Results::default(), |mut acc, result| {
            match result {
                Ok(accounts) => {
                    assert_eq!(accounts.len(), 0);
                    acc.successes += 1;
                }
                Err(tb::PacketStatus::ClientEvicted) => acc.evictions += 1,
                Err(_) => acc.failures += 1,
            }
            acc
        });

    block_on(async {
        for client in clients {
            let client = Arc::try_unwrap(client).expect("arc unwrap");
            client.close().await;
        }
        Ok::<_, anyhow::Error>(())
    })?;

    eprintln!(
        "successes={} evictions={} failures={}",
        results.successes, results.evictions, results.failures,
    );

    assert_eq!(
        results.successes + results.evictions + results.failures,
        CLIENTS * THREADS_PER_CLIENT * REQUESTS_PER_THREAD,
    );
    // This may not actually trigger eviction.
    assert!(results.successes > 0);
    assert_eq!(results.failures, 0);

    Ok(())
}

#[test]
fn eviction_stress_2() -> anyhow::Result<()> {
    use futures::executor::LocalPool;
    use futures::task::LocalSpawnExt;
    use std::cell::Cell;
    use std::rc::Rc;

    const CLIENTS: usize = 128;
    const EVICTIONS_NEEDED: usize = 64;
    const REQUEST_LIMIT: usize = 100;

    let server = TestDb::new_development("eviction_stress_2")?;
    let address = server.address();

    let clients: Vec<Rc<tb::Client>> = (0..CLIENTS)
        .map(|_| Ok(Rc::new(tb::Client::new(0, &address)?)))
        .collect::<anyhow::Result<_>>()?;

    let evictions = Rc::new(Cell::new(0usize));
    let successes = Rc::new(Cell::new(0usize));

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();

    for client in &clients {
        let client = client.clone();
        let evictions = evictions.clone();
        let successes = successes.clone();
        spawner
            .spawn_local(async move {
                for _ in 0..REQUEST_LIMIT {
                    if evictions.get() >= EVICTIONS_NEEDED {
                        break;
                    }
                    match client.lookup_accounts(&[tb::id()]).await {
                        Ok(_) => successes.set(successes.get() + 1),
                        Err(tb::PacketStatus::ClientEvicted) => evictions.set(evictions.get() + 1),
                        Err(e) => panic!("{e}"),
                    }
                }
            })
            .unwrap();
    }

    pool.run();

    let evictions = evictions.get();
    let successes = successes.get();
    eprintln!("successes={successes} evictions={evictions}");

    assert!(evictions >= EVICTIONS_NEEDED);
    assert!(successes > 0);

    for client in clients {
        let client = Rc::try_unwrap(client).expect("rc unwrap");
        block_on(client.close());
    }

    Ok(())
}
