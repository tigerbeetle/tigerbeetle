#![allow(unused)]

// section:imports
use tigerbeetle as tb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    futures::executor::block_on(main_async())
}

async fn main_async() -> Result<(), Box<dyn std::error::Error>> {
    println!("hello world");
    // endsection:imports

    // section:client
    let cluster_id = 0;
    let replica_address = std::env::var("TB_ADDRESS")
        .ok()
        .unwrap_or_else(|| String::from("3000"));
    let client = tb::Client::new(cluster_id, &replica_address)?;
    // endsection:client

    {
        // section:create-accounts
        let account_errors = client
            .create_accounts(&[tb::Account {
                id: tb::id(),
                ledger: 1,
                code: 718,
                ..Default::default()
            }])
            .await?;
        // Error handling omitted.
        // endsection:create-accounts
    }

    {
        // section:account-flags
        let account0 = tb::Account {
            id: 100,
            ledger: 1,
            code: 718,
            flags: tb::AccountFlags::DebitsMustNotExceedCredits | tb::AccountFlags::Linked,
            ..Default::default()
        };
        let account1 = tb::Account {
            id: 101,
            ledger: 1,
            code: 718,
            flags: tb::AccountFlags::History,
            ..Default::default()
        };

        let account_errors = client.create_accounts(&[account0, account1]).await?;
        // Error handling omitted.
        // endsection:account-flags
    }

    {
        // section:create-accounts-errors
        let account0 = tb::Account {
            id: 102,
            ledger: 1,
            code: 718,
            ..Default::default()
        };
        let account1 = tb::Account {
            id: 103,
            ledger: 1,
            code: 718,
            ..Default::default()
        };
        let account2 = tb::Account {
            id: 104,
            ledger: 1,
            code: 718,
            ..Default::default()
        };

        let account_errors = client
            .create_accounts(&[account0, account1, account2])
            .await?;

        assert!(account_errors.len() <= 3);

        for err in account_errors {
            match err.result {
                tb::CreateAccountResult::Exists => {
                    println!("Batch account at {} already exists.", err.index);
                }
                _ => {
                    eprintln!(
                        "Batch account at {} failed to create: {:?}",
                        err.index, err.result
                    );
                }
            }
        }
        // endsection:create-accounts-errors
    }

    {
        // section:lookup-accounts
        let accounts = client.lookup_accounts(&[100, 101]).await?;
        // endsection:lookup-accounts
    }

    {
        // section:create-transfers
        let transfers = vec![tb::Transfer {
            id: tb::id(),
            debit_account_id: 101,
            credit_account_id: 102,
            amount: 10,
            ledger: 1,
            code: 1,
            ..Default::default()
        }];

        let transfer_errors = client.create_transfers(&transfers).await?;
        // Error handling omitted.
        // endsection:create-transfers
    }

    {
        // section:create-transfers-errors
        let transfers = vec![
            tb::Transfer {
                id: 1,
                debit_account_id: 101,
                credit_account_id: 102,
                amount: 10,
                ledger: 1,
                code: 1,
                ..Default::default()
            },
            tb::Transfer {
                id: 2,
                debit_account_id: 101,
                credit_account_id: 102,
                amount: 10,
                ledger: 1,
                code: 1,
                ..Default::default()
            },
            tb::Transfer {
                id: 3,
                debit_account_id: 101,
                credit_account_id: 102,
                amount: 10,
                ledger: 1,
                code: 1,
                ..Default::default()
            },
        ];

        let transfer_errors = client.create_transfers(&transfers).await?;

        for err in transfer_errors {
            match err.result {
                tb::CreateTransferResult::Exists => {
                    println!("Batch transfer at {} already exists.", err.index);
                }
                _ => {
                    eprintln!(
                        "Batch transfer at {} failed to create: {:?}",
                        err.index, err.result
                    );
                }
            }
        }
        // endsection:create-transfers-errors
    }

    {
        // section:no-batch
        let batch: Vec<tb::Transfer> = vec![];
        for transfer in &batch {
            let transfer_errors = client.create_transfers(&[*transfer]).await?;
            // Error handling omitted.
        }
        // endsection:no-batch
    }

    {
        // section:batch
        let transfers: Vec<tb::Transfer> = vec![];
        const BATCH_SIZE: usize = 8189;
        for batch in transfers.chunks(BATCH_SIZE) {
            let transfer_errors = client.create_transfers(batch).await?;
            // Error handling omitted.
        }
        // endsection:batch
    }

    {
        // section:transfer-flags-link
        let transfer0 = tb::Transfer {
            id: 4,
            debit_account_id: 101,
            credit_account_id: 102,
            amount: 10,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::Linked,
            ..Default::default()
        };
        let transfer1 = tb::Transfer {
            id: 5,
            debit_account_id: 101,
            credit_account_id: 102,
            amount: 10,
            ledger: 1,
            code: 1,
            ..Default::default()
        };

        let transfer_errors = client.create_transfers(&[transfer0, transfer1]).await?;
        // Error handling omitted.
        // endsection:transfer-flags-link
    }

    {
        // section:transfer-flags-post
        let transfer0 = tb::Transfer {
            id: 6,
            debit_account_id: 101,
            credit_account_id: 102,
            amount: 10,
            ledger: 1,
            code: 1,
            ..Default::default()
        };

        let transfer_errors = client.create_transfers(&[transfer0]).await?;
        // Error handling omitted.

        let transfer1 = tb::Transfer {
            id: 7,
            amount: u128::MAX,
            pending_id: 6,
            flags: tb::TransferFlags::PostPendingTransfer,
            ..Default::default()
        };

        let transfer_errors = client.create_transfers(&[transfer1]).await?;
        // Error handling omitted.
        // endsection:transfer-flags-post
    }

    {
        // section:transfer-flags-void
        let transfer0 = tb::Transfer {
            id: 8,
            debit_account_id: 101,
            credit_account_id: 102,
            amount: 10,
            ledger: 1,
            code: 1,
            ..Default::default()
        };

        let transfer_errors = client.create_transfers(&[transfer0]).await?;
        // Error handling omitted.

        let transfer1 = tb::Transfer {
            id: 9,
            amount: 0,
            pending_id: 8,
            flags: tb::TransferFlags::VoidPendingTransfer,
            ..Default::default()
        };

        let transfer_errors = client.create_transfers(&[transfer1]).await?;
        // Error handling omitted.
        // endsection:transfer-flags-void
    }

    {
        // section:lookup-transfers
        let transfers = client.lookup_transfers(&[1, 2]).await?;
        // endsection:lookup-transfers
    }

    {
        // section:get-account-transfers
        let filter = tb::AccountFilter {
            account_id: 2,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            code: 0,
            reserved: Default::default(),
            timestamp_min: 0,
            timestamp_max: 0,
            limit: 10,
            flags: tb::AccountFilterFlags::Debits
                | tb::AccountFilterFlags::Credits
                | tb::AccountFilterFlags::Reversed,
        };

        let transfers = client.get_account_transfers(filter).await?;
        // endsection:get-account-transfers
    }

    {
        // section:get-account-balances
        let filter = tb::AccountFilter {
            account_id: 2,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            code: 0,
            reserved: Default::default(),
            timestamp_min: 0,
            timestamp_max: 0,
            limit: 10,
            flags: tb::AccountFilterFlags::Debits
                | tb::AccountFilterFlags::Credits
                | tb::AccountFilterFlags::Reversed,
        };

        let account_balances = client.get_account_balances(filter).await?;
        // endsection:get-account-balances
    }

    {
        // section:query-accounts
        let filter = tb::QueryFilter {
            user_data_128: 1000,
            user_data_64: 100,
            user_data_32: 10,
            code: 1,
            ledger: 0,
            reserved: Default::default(),
            timestamp_min: 0,
            timestamp_max: 0,
            limit: 10,
            flags: tb::QueryFilterFlags::Reversed,
        };

        let accounts = client.query_accounts(filter).await?;
        // endsection:query-accounts
    }

    {
        // section:query-transfers
        let filter = tb::QueryFilter {
            user_data_128: 1000,
            user_data_64: 100,
            user_data_32: 10,
            code: 1,
            ledger: 0,
            reserved: Default::default(),
            timestamp_min: 0,
            timestamp_max: 0,
            limit: 10,
            flags: tb::QueryFilterFlags::Reversed,
        };

        let transfers = client.query_transfers(filter).await?;
        // endsection:query-transfers
    }

    {
        // section:linked-events
        let mut batch = vec![];
        let linked_flag = tb::TransferFlags::Linked;

        // An individual transfer (successful):
        batch.push(tb::Transfer {
            id: 1,
            ..Default::default()
        });

        // A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
        batch.push(tb::Transfer {
            id: 2,
            flags: linked_flag,
            ..Default::default()
        });
        batch.push(tb::Transfer {
            id: 3,
            flags: linked_flag,
            ..Default::default()
        });
        batch.push(tb::Transfer {
            id: 2,
            flags: linked_flag,
            ..Default::default()
        });
        batch.push(tb::Transfer {
            id: 4,
            ..Default::default()
        });

        // An individual transfer (successful):
        // This should not see any effect from the failed chain above.
        batch.push(tb::Transfer {
            id: 2,
            ..Default::default()
        });

        // A chain of 2 transfers (the first transfer fails the chain):
        batch.push(tb::Transfer {
            id: 2,
            flags: linked_flag,
            ..Default::default()
        });
        batch.push(tb::Transfer {
            id: 3,
            ..Default::default()
        });

        // A chain of 2 transfers (successful):
        batch.push(tb::Transfer {
            id: 3,
            flags: linked_flag,
            ..Default::default()
        });
        batch.push(tb::Transfer {
            id: 4,
            ..Default::default()
        });

        let transfer_errors = client.create_transfers(&batch).await?;
        // Error handling omitted.
        // endsection:linked-events
    }

    {
        // section:imported-events
        // External source of time.
        let mut historical_timestamp: u64 = 0;
        let historical_accounts: Vec<tb::Account> = vec![]; // Loaded from an external source.
        let historical_transfers: Vec<tb::Transfer> = vec![]; // Loaded from an external source.

        // First, load and import all accounts with their timestamps from the historical source.
        let mut accounts_batch = vec![];
        for (index, mut account) in historical_accounts.into_iter().enumerate() {
            // Set a unique and strictly increasing timestamp.
            historical_timestamp += 1;
            account.timestamp = historical_timestamp;

            account.flags = if index < accounts_batch.len() - 1 {
                tb::AccountFlags::Imported | tb::AccountFlags::Linked
            } else {
                tb::AccountFlags::Imported
            };

            accounts_batch.push(account);
        }

        let account_errors = client.create_accounts(&accounts_batch).await?;
        // Error handling omitted.

        // Then, load and import all transfers with their timestamps from the historical source.
        let mut transfers_batch = vec![];
        for (index, mut transfer) in historical_transfers.into_iter().enumerate() {
            // Set a unique and strictly increasing timestamp.
            historical_timestamp += 1;
            transfer.timestamp = historical_timestamp;

            transfer.flags = if index < transfers_batch.len() - 1 {
                tb::TransferFlags::Imported | tb::TransferFlags::Linked
            } else {
                tb::TransferFlags::Imported
            };

            transfers_batch.push(transfer);
        }

        let transfer_errors = client.create_transfers(&transfers_batch).await?;
        // Error handling omitted.
        // Since it is a linked chain, in case of any error the entire batch is rolled back and can be retried
        // with the same historical timestamps without regressing the cluster timestamp.
        // endsection:imported-events
    }

    Ok(())
    // section:imports
}
// endsection:imports
