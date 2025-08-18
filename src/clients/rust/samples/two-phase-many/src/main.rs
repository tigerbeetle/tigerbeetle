use tigerbeetle as tb;

async fn assert_account_balances(
    client: &tb::Client,
    expected_accounts: &[tb::Account],
    debug_msg: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let ids: Vec<u128> = expected_accounts.iter().map(|a| a.id).collect();
    let found_accounts = client.lookup_accounts(&ids).await?;
    assert_eq!(expected_accounts.len(), found_accounts.len(), "accounts");

    for found_account in &found_accounts {
        let mut requested = false;
        for expected_account in expected_accounts {
            if expected_account.id == found_account.id {
                requested = true;
                assert_eq!(
                    expected_account.debits_posted, found_account.debits_posted,
                    "account {} debits, {}",
                    expected_account.id, debug_msg
                );
                assert_eq!(
                    expected_account.credits_posted, found_account.credits_posted,
                    "account {} credits, {}",
                    expected_account.id, debug_msg
                );
                assert_eq!(
                    expected_account.debits_pending, found_account.debits_pending,
                    "account {} debits pending, {}",
                    expected_account.id, debug_msg
                );
                assert_eq!(
                    expected_account.credits_pending, found_account.credits_pending,
                    "account {} credits pending, {}",
                    expected_account.id, debug_msg
                );
            }
        }

        if !requested {
            panic!("Unexpected account: {}", found_account.id);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // Start five pending transfers
    let transfers = vec![
        tb::Transfer {
            id: 1,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 100,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::Pending,
            ..Default::default()
        },
        tb::Transfer {
            id: 2,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 200,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::Pending,
            ..Default::default()
        },
        tb::Transfer {
            id: 3,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 300,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::Pending,
            ..Default::default()
        },
        tb::Transfer {
            id: 4,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 400,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::Pending,
            ..Default::default()
        },
        tb::Transfer {
            id: 5,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 500,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::Pending,
            ..Default::default()
        },
    ];

    let transfer_errors = client.create_transfers(&transfers).await?;
    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Validate accounts pending and posted debits/credits before finishing the two-phase transfer
    assert_account_balances(
        &client,
        &[
            tb::Account {
                id: 1,
                debits_posted: 0,
                credits_posted: 0,
                debits_pending: 1500,
                credits_pending: 0,
                ..Default::default()
            },
            tb::Account {
                id: 2,
                debits_posted: 0,
                credits_posted: 0,
                debits_pending: 0,
                credits_pending: 1500,
                ..Default::default()
            },
        ],
        "after starting 5 pending transfers",
    )
    .await?;

    // Create a 6th transfer posting the 1st transfer
    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 6,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 100,
            pending_id: 1,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::PostPendingTransfer,
            ..Default::default()
        }])
        .await?;

    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Validate account balances after posting 1st pending transfer
    assert_account_balances(
        &client,
        &[
            tb::Account {
                id: 1,
                debits_posted: 100,
                credits_posted: 0,
                debits_pending: 1400,
                credits_pending: 0,
                ..Default::default()
            },
            tb::Account {
                id: 2,
                debits_posted: 0,
                credits_posted: 100,
                debits_pending: 0,
                credits_pending: 1400,
                ..Default::default()
            },
        ],
        "after completing 1 pending transfer",
    )
    .await?;

    // Create a 7th transfer voiding the 2nd transfer
    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 7,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 200,
            pending_id: 2,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::VoidPendingTransfer,
            ..Default::default()
        }])
        .await?;

    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Validate account balances after voiding 2nd pending transfer
    assert_account_balances(
        &client,
        &[
            tb::Account {
                id: 1,
                debits_posted: 100,
                credits_posted: 0,
                debits_pending: 1200,
                credits_pending: 0,
                ..Default::default()
            },
            tb::Account {
                id: 2,
                debits_posted: 0,
                credits_posted: 100,
                debits_pending: 0,
                credits_pending: 1200,
                ..Default::default()
            },
        ],
        "after completing 2 pending transfers",
    )
    .await?;

    // Create a 8th transfer posting the 3rd transfer
    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 8,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 300,
            pending_id: 3,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::PostPendingTransfer,
            ..Default::default()
        }])
        .await?;

    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Validate account balances after posting 3rd pending transfer
    assert_account_balances(
        &client,
        &[
            tb::Account {
                id: 1,
                debits_posted: 400,
                credits_posted: 0,
                debits_pending: 900,
                credits_pending: 0,
                ..Default::default()
            },
            tb::Account {
                id: 2,
                debits_posted: 0,
                credits_posted: 400,
                debits_pending: 0,
                credits_pending: 900,
                ..Default::default()
            },
        ],
        "after completing 3 pending transfers",
    )
    .await?;

    // Create a 9th transfer voiding the 4th transfer
    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 9,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 400,
            pending_id: 4,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::VoidPendingTransfer,
            ..Default::default()
        }])
        .await?;

    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Validate account balances after voiding 4th pending transfer
    assert_account_balances(
        &client,
        &[
            tb::Account {
                id: 1,
                debits_posted: 400,
                credits_posted: 0,
                debits_pending: 500,
                credits_pending: 0,
                ..Default::default()
            },
            tb::Account {
                id: 2,
                debits_posted: 0,
                credits_posted: 400,
                debits_pending: 0,
                credits_pending: 500,
                ..Default::default()
            },
        ],
        "after completing 4 pending transfers",
    )
    .await?;

    // Create a 10th transfer posting the 5th transfer
    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 10,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 500,
            pending_id: 5,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::PostPendingTransfer,
            ..Default::default()
        }])
        .await?;

    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Validate account balances after posting 5th pending transfer
    assert_account_balances(
        &client,
        &[
            tb::Account {
                id: 1,
                debits_posted: 900,
                credits_posted: 0,
                debits_pending: 0,
                credits_pending: 0,
                ..Default::default()
            },
            tb::Account {
                id: 2,
                debits_posted: 0,
                credits_posted: 900,
                debits_pending: 0,
                credits_pending: 0,
                ..Default::default()
            },
        ],
        "after completing 5 pending transfers",
    )
    .await?;

    println!("ok");
    Ok(())
}
