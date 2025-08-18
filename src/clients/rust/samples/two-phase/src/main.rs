use tigerbeetle as tb;

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

    // Start a pending transfer
    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 1,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 500,
            ledger: 1,
            code: 1,
            flags: tb::TransferFlags::Pending,
            ..Default::default()
        }])
        .await?;

    for error in &transfer_errors {
        eprintln!("Error creating transfer: {:?}", error.result);
    }
    assert!(transfer_errors.is_empty());

    // Validate accounts pending and posted debits/credits before finishing the two-phase transfer
    let accounts = client.lookup_accounts(&[1, 2]).await?;
    assert_eq!(accounts.len(), 2);

    for account in &accounts {
        if account.id == 1 {
            assert_eq!(account.debits_posted, 0, "account 1 debits, before posted");
            assert_eq!(
                account.credits_posted, 0,
                "account 1 credits, before posted"
            );
            assert_eq!(
                account.debits_pending, 500,
                "account 1 debits pending, before posted"
            );
            assert_eq!(
                account.credits_pending, 0,
                "account 1 credits pending, before posted"
            );
        } else if account.id == 2 {
            assert_eq!(account.debits_posted, 0, "account 2 debits, before posted");
            assert_eq!(
                account.credits_posted, 0,
                "account 2 credits, before posted"
            );
            assert_eq!(
                account.debits_pending, 0,
                "account 2 debits pending, before posted"
            );
            assert_eq!(
                account.credits_pending, 500,
                "account 2 credits pending, before posted"
            );
        } else {
            panic!("Unexpected account: {}", account.id);
        }
    }

    // Create a second transfer simply posting the first transfer
    let transfer_errors = client
        .create_transfers(&[tb::Transfer {
            id: 2,
            debit_account_id: 1,
            credit_account_id: 2,
            amount: 500,
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

    // Validate the contents of all transfers
    let transfers = client.lookup_transfers(&[1, 2]).await?;
    assert_eq!(transfers.len(), 2);

    for transfer in &transfers {
        if transfer.id == 1 {
            assert!(
                transfer.flags.contains(tb::TransferFlags::Pending),
                "transfer 1 pending"
            );
            assert!(
                !transfer
                    .flags
                    .contains(tb::TransferFlags::PostPendingTransfer),
                "transfer 1 post_pending_transfer"
            );
        } else if transfer.id == 2 {
            assert!(
                !transfer.flags.contains(tb::TransferFlags::Pending),
                "transfer 2 pending"
            );
            assert!(
                transfer
                    .flags
                    .contains(tb::TransferFlags::PostPendingTransfer),
                "transfer 2 post_pending_transfer"
            );
        } else {
            panic!("Unknown transfer: {}", transfer.id);
        }
    }

    // Validate accounts pending and posted debits/credits after finishing the two-phase transfer
    let accounts = client.lookup_accounts(&[1, 2]).await?;
    assert_eq!(accounts.len(), 2);

    for account in &accounts {
        if account.id == 1 {
            assert_eq!(account.debits_posted, 500, "account 1 debits");
            assert_eq!(account.credits_posted, 0, "account 1 credits");
            assert_eq!(account.debits_pending, 0, "account 1 debits pending");
            assert_eq!(account.credits_pending, 0, "account 1 credits pending");
        } else if account.id == 2 {
            assert_eq!(account.debits_posted, 0, "account 2 debits");
            assert_eq!(account.credits_posted, 500, "account 2 credits");
            assert_eq!(account.debits_pending, 0, "account 2 debits pending");
            assert_eq!(account.credits_pending, 0, "account 2 credits pending");
        } else {
            panic!("Unexpected account: {}", account.id);
        }
    }

    println!("ok");
    Ok(())
}
