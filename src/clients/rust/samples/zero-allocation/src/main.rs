use std::error::Error;

use tigerbeetle as tb;

fn main() -> Result<(), Box<dyn Error>> {
    let cluster_id = 0;
    let tigerbeetle_port = std::env::var("TB_ADDRESS").unwrap_or_else(|_| "3000".to_string());
    let client = tb::Client::new(cluster_id, &tigerbeetle_port)?;

    let batch_size_max: usize = 128;
    let account_count: usize = 128;
    let transfer_count: usize = 5000;

    assert!(account_count >= 2);
    assert!(account_count <= batch_size_max);

    // Pre-allocate all memory, mutably, since these will be continuously re-assigned for reuse:
    // - (1) The completion state. For multiple outstanding requests, you can create a pool of these.
    //       The completion state is type-erased and can be reused for different operation types.
    let mut completion = tb::Completion::new();

    // - (2) Input and result vectors for account creation
    let mut accounts: Vec<tb::Account> = Vec::with_capacity(batch_size_max);
    let mut accounts_results: Vec<tb::CreateAccountResult> = Vec::with_capacity(batch_size_max);

    // - (3) Input and result vectors for transfer creation
    let mut transfers: Vec<tb::Transfer> = Vec::with_capacity(batch_size_max);
    let mut transfers_results: Vec<tb::CreateTransferResult> = Vec::with_capacity(batch_size_max);

    futures::executor::block_on(async {
        // Phase 1: create all accounts between which we will transfer funds.
        for n in 0..account_count {
            accounts.push(tb::Account {
                id: 1 + n as u128,
                ledger: 1,
                code: 1,
                ..Default::default()
            });
        }

        // Reusable methods return the completion and buffers to reuse in future requests.
        // Unlike the convenience methods, they do not allocate anything per request, but
        // need to take ownership of the passed buffers. They cannot borrow. You can read
        //
        //      https://without.boats/blog/io-uring/#the-kernel-must-own-the-buffer
        //
        // for a good mental model, which is not specific to io_uring and applicable here.
        (completion, accounts, accounts_results) = client
            .create_accounts_reusable(completion, accounts, accounts_results)
            .await?;

        // The future's output type is Result<(Completion, InputVec, OutputVec), PacketError>.
        // On packet error, the completion and buffers are not returned for reuse, since this
        // would significantly complicate the API, while packet errors are rarely recoverable.
        assert_eq!(accounts_results.len(), accounts.len());
        for account_result in &accounts_results {
            if account_result.status != tb::CreateAccountStatus::Created {
                return Err(format!("error creating account: {}", account_result.status).into());
            }
        }

        // Phase 2: Create transfers between random accounts.
        let t_start = std::time::Instant::now();
        let mut processed = 0;
        while processed < transfer_count {
            transfers.clear();
            transfers_results.clear();

            let batch_size = (transfer_count - processed).min(batch_size_max);
            processed += batch_size;

            for _ in 0..batch_size {
                transfers.push(tb::Transfer {
                    id: tb::id(),
                    debit_account_id: 1,
                    credit_account_id: 2,
                    amount: 1,
                    ledger: 1,
                    code: 1,
                    ..Default::default()
                });
            }

            (completion, transfers, transfers_results) = client
                .create_transfers_reusable(completion, transfers, transfers_results)
                .await?;

            assert_eq!(transfers_results.len(), batch_size);
            for transfer_result in &transfers_results {
                if transfer_result.status != tb::CreateTransferStatus::Created {
                    return Err(
                        format!("error creating transfer: {}", transfer_result.status).into(),
                    );
                }
            }
        }

        let elapsed = t_start.elapsed();
        println!(
            "elapsed: {:?} | transfers: {} | tps: {}",
            elapsed,
            processed,
            processed as f64 / elapsed.as_secs_f64(),
        );
        Ok(())
    })
}
