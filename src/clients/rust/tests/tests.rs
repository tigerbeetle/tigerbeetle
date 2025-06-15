use std::env;
use std::env::consts::EXE_SUFFIX;
use std::io::{BufRead as _, BufReader};
use std::mem;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier, LazyLock};

use futures::executor::block_on;

use tigerbeetle as tb;

// Singleton test database.
static TEST_DB: LazyLock<TestDb> =
    LazyLock::new(|| TestDb::new().expect("couldn't start test database"));

struct TestDb {
    port: u16,
    // Keep the server's stdin handle open as long as the test process is running,
    // at which point the server will terminate and delete its backing file.
    _server: Child,
}

impl TestDb {
    fn new() -> anyhow::Result<TestDb> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");

        let tigerbeetle_bin = format!("{manifest_dir}/../../../tigerbeetle{EXE_SUFFIX}");
        let work_dir = env!("CARGO_TARGET_TMPDIR");
        let database_name = format!("0_0.{:016x}.tigerbeetle", tb::id() as u64);

        let mut cmd = Command::new(&tigerbeetle_bin);
        cmd.current_dir(&work_dir);
        cmd.args([
            "format",
            "--replica-count=1",
            "--replica=0",
            "--cluster=0",
            &database_name,
        ]);
        let status = cmd.status()?;

        assert!(status.success());

        let mut cmd = Command::new(&tigerbeetle_bin);
        cmd.current_dir(&work_dir);
        cmd.args([
            "start",
            // magic address 0: tell us the port to use,
            // shutdown and delete db when stdin closes
            "--addresses=0",
            &database_name,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());

        let mut server = cmd.spawn()?;
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
}

fn test_client() -> anyhow::Result<tb::Client> {
    let address = &format!("127.0.0.1:{}", TEST_DB.port);
    let client = tb::Client::new(0, address)?;

    Ok(client)
}

const TEST_LEDGER: u32 = 10;
const TEST_CODE: u16 = 20;

#[test]
fn smoke() -> anyhow::Result<()> {
    let account_id1 = tb::id();
    let account_id2 = tb::id();
    let transfer_id1 = tb::id();

    let account_id2_user_data_32 = 4;
    let transfer_id1_user_data_32 = 5;

    block_on(async {
        let client = test_client()?;

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
        // Let's at least talk to the server before dropping
        let _ = client.create_accounts(&[]).await?;
        client.close().await;
        Ok(())
    })
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
fn zero_events() -> anyhow::Result<()> {
    let client = test_client()?;

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
    let client = test_client()?;

    block_on(async {
        let result = client
            .create_accounts(&[
                tb::Account {
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
                },
                tb::Account {
                    id: tb::id(),
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
                },
                tb::Account {
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
                    code: 0, // err
                    flags: tb::AccountFlags::History,
                    timestamp: 0,
                },
                tb::Account {
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
        for result in response {
            assert_eq!(result, tb::CreateAccountResult::Ok);
        }
    }

    Ok(())
}
