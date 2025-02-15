use futures::executor::block_on;
use std::env;
use std::env::consts::EXE_SUFFIX;
use std::io::{BufRead as _, BufReader};
use std::mem;
use std::process::{Child, Command, Stdio};
use tempfile::TempDir;

use tigerbeetle as tb;

struct TestHarness {
    tigerbeetle_bin: String,
    temp_dir: TempDir,
    server: Option<Child>,
}

impl TestHarness {
    fn new(name: &str) -> anyhow::Result<TestHarness> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");

        let tigerbeetle_bin = format!("{manifest_dir}/../../../tigerbeetle{EXE_SUFFIX}");

        let work_dir = env!("CARGO_TARGET_TMPDIR");
        let temp_dir = TempDir::with_prefix_in(name, &work_dir)?;

        Ok(TestHarness {
            tigerbeetle_bin,
            temp_dir,
            server: None,
        })
    }

    fn prepare_database(&self) -> anyhow::Result<()> {
        let mut cmd = Command::new(&self.tigerbeetle_bin);
        cmd.current_dir(self.temp_dir.path());
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
        cmd.current_dir(self.temp_dir.path());
        cmd.args([
            "start",
            "--addresses=0", // tell us the port to use
            "0_0.tigerbeetle",
        ])
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
    }
}

#[test]
fn smoke() -> anyhow::Result<()> {
    let ledger = 1;
    let code = 1;

    let account_id1 = 1;
    let account_id2 = 2;
    let transfer_id1 = 3;

    let account_id2_user_data_32 = 4;
    let transfer_id1_user_data_32 = 5;

    block_on(async {
        let mut harness = TestHarness::new("smoke")?;
        harness.prepare_database()?;
        let port_number = harness.serve()?;

        let address = &format!("127.0.0.1:{}", port_number);
        let client = tb::Client::new(0, address)?;

        {
            let res = client
                .submit_create_accounts(&[
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
                        ledger,
                        code,
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
                        ledger,
                        code,
                        flags: tb::AccountFlags::History,
                        timestamp: 0,
                    },
                ])
                .await?;

            assert_eq!(res.len(), 2);

            assert!(res[0].is_ok());
            assert!(res[1].is_ok());
        }

        {
            let res = client
                .submit_create_transfers(&[tb::Transfer {
                    id: transfer_id1,
                    debit_account_id: account_id1,
                    credit_account_id: account_id2,
                    amount: 10,
                    pending_id: 0,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: transfer_id1_user_data_32,
                    timeout: 0,
                    ledger,
                    code,
                    flags: tb::TransferFlags::default(),
                    timestamp: 0,
                }])
                .await?;

            assert_eq!(res.len(), 1);
            assert!(res[0].is_ok());
        }

        {
            let res = client
                .submit_lookup_accounts(&[account_id1, account_id2])
                .await?;

            assert_eq!(res.len(), 2);
            let res_account1 = res[0].unwrap();
            let res_account2 = res[1].unwrap();

            assert_eq!(res_account1.id, account_id1);
            assert_eq!(res_account1.debits_posted, 10);
            assert_eq!(res_account1.credits_posted, 0);
            assert_eq!(res_account2.id, account_id2);
            assert_eq!(res_account2.debits_posted, 0);
            assert_eq!(res_account2.credits_posted, 10);
        }

        {
            let res = client.submit_lookup_transfers(&[transfer_id1]).await?;

            assert_eq!(res.len(), 1);
            let res_transfer1 = res[0].unwrap();

            assert_eq!(res_transfer1.id, transfer_id1);
            assert_eq!(res_transfer1.debit_account_id, account_id1);
            assert_eq!(res_transfer1.credit_account_id, account_id2);
            assert_eq!(res_transfer1.amount, 10);
        }

        {
            let res = client
                .submit_get_account_transfers(tb::AccountFilter {
                    account_id: account_id1,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    code,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::AccountFilterFlags::Credits | tb::AccountFilterFlags::Debits,
                })
                .await?;

            assert_eq!(res.len(), 1);

            let res_transfer = &res[0];

            assert_eq!(res_transfer.id, transfer_id1);
            assert_eq!(res_transfer.debit_account_id, account_id1);
            assert_eq!(res_transfer.credit_account_id, account_id2);
            assert_eq!(res_transfer.amount, 10);
        }

        {
            let res = client
                .submit_get_account_balances(tb::AccountFilter {
                    account_id: account_id1,
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: 0,
                    code,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::AccountFilterFlags::Credits | tb::AccountFilterFlags::Debits,
                })
                .await?;

            assert_eq!(res.len(), 1);

            let res_balance_1 = &res[0];

            assert_eq!(res_balance_1.debits_posted, 10);
            assert_eq!(res_balance_1.credits_posted, 0);
        }

        {
            let res = client
                .submit_query_accounts(tb::QueryFilter {
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: account_id2_user_data_32,
                    ledger,
                    code,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::QueryFilterFlags::default(),
                })
                .await?;

            assert_eq!(res.len(), 1);

            let res_account = &res[0];

            assert_eq!(res_account.id, account_id2);
        }

        {
            let res = client
                .submit_query_transfers(tb::QueryFilter {
                    user_data_128: 0,
                    user_data_64: 0,
                    user_data_32: transfer_id1_user_data_32,
                    ledger,
                    code,
                    reserved: tb::Reserved::default(),
                    timestamp_min: 0,
                    timestamp_max: 0,
                    limit: 10,
                    flags: tb::QueryFilterFlags::default(),
                })
                .await?;

            assert_eq!(res.len(), 1);

            let res_transfer = &res[0];

            assert_eq!(res_transfer.id, transfer_id1);
        }

        Ok(())
    })
}
