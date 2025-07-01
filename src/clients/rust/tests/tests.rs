use std::env;
use std::env::consts::EXE_SUFFIX;
use std::io::{BufRead as _, BufReader};
use std::mem;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier, LazyLock};

use futures::executor::block_on;
use futures::pin_mut;
use futures::{Stream, StreamExt};

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

        // NB: There is one test database shared between all tests, and reused
        // between test runs. If the tests choose their IDs correctly there
        // should never be any collisions, and that one database should work
        // forever, just taking up a lot of space.
        let tigerbeetle_bin = format!("{manifest_dir}/../../../tigerbeetle{EXE_SUFFIX}");
        let work_dir = env!("CARGO_TARGET_TMPDIR");
        let database_name = "0_0.testdb.tigerbeetle";

        if !std::fs::exists(&format!("{work_dir}/{database_name}"))? {
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
        }

        let mut cmd = Command::new(&tigerbeetle_bin);
        cmd.current_dir(&work_dir);
        cmd.args([
            "start",
            // magic address 0: tell us the port to use,
            // shutdown when stdin closes
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
// NB: If you change this also change the paging example in the crate docs!
fn get_account_transfers_paged<'s>(
    client: &'s tb::Client,
    event: tb::AccountFilter,
) -> impl Stream<Item = Result<Vec<tb::Transfer>, tb::PacketStatus>> + use<'s> {
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
                        assert_ne!(timestamp_begin_next, u64::max_value());
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
        assert_eq!(account_results[0], tb::CreateAccountResult::Ok);
        assert_eq!(account_results[1], tb::CreateAccountResult::Ok);

        for transfers in transfers.chunks(batch_size) {
            let transfer_results = client.create_transfers(transfers).await?;
            assert!(transfer_results
                .into_iter()
                .all(|t| t == tb::CreateTransferResult::Ok));
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
