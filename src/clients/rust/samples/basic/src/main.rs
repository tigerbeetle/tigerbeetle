use tigerbeetle as tb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    futures::executor::block_on(main_async())
}

async fn main_async() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("TB_ADDRESS").unwrap_or_else(|_| "3000".to_string());
    let client = tb::Client::new(0, &port)?;

    // Create two accounts
    let account_errors = client
        .create_accounts(&[
            tb::Account {
                id: 1,
                ledger: 1,
                code: 1,
                ..Default::default()
            },
            tb::Account {
                id: 2,
                ledger: 1,
                code: 1,
                ..Default::default()
            },
        ])
        .await?;

    for error in &account_errors {
        eprintln!("Error creating account {}: {:?}", error.index, error.result);
    }
    assert!(account_errors.is_empty());

    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 1,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 10,
            ledger: 1,
            code: 1,
            ..Default::default()
        }])
        .await?;

    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Check the sums for both accounts
    let accounts = client.lookup_accounts(&[1, 2]).await?;
    assert_eq!(accounts.len(), 2);

    for account in accounts {
        if account.id == 1 {
            assert_eq!(account.debits_posted, 10);
            assert_eq!(account.credits_posted, 0);
        } else if account.id == 2 {
            assert_eq!(account.debits_posted, 0);
            assert_eq!(account.credits_posted, 10);
        } else {
            panic!("Unexpected account");
        }
    }

    println!("ok");
    Ok(())
}
