use tigerbeetle as tb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(main_async())
}

async fn main_async() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("TB_ADDRESS").unwrap_or_else(|_| "3000".to_string());
    let client = tb::Client::new(0, &port)?;

    // Create accounts.
    let account_dr_id = tb::id();
    let account_cr_id = tb::id();

    let account_results = client
        .create_accounts(&[
            tb::Account {
                id: account_dr_id,
                ledger: 1,
                code: 1,
                ..Default::default()
            },
            tb::Account {
                id: account_cr_id,
                ledger: 1,
                code: 1,
                ..Default::default()
            },
        ])
        .await?;

    assert!(account_results.len() == 2);
    assert!(account_results[0].status == tb::CreateAccountStatus::Created);
    assert!(account_results[1].status == tb::CreateAccountStatus::Created);

    // Create pending transfer.
    let transfer_pending_id = tb::id();
    let transfer_results = client
        .create_transfers(&[tb::Transfer {
            id: transfer_pending_id,
            debit_account_id: account_dr_id,
            credit_account_id: account_cr_id,
            ledger: 1,
            code: 1,
            amount: 500,
            flags: tb::TransferFlags::Pending,
            ..Default::default()
        }])
        .await?;

    assert!(transfer_results.len() == 1);
    assert!(transfer_results[0].status == tb::CreateTransferStatus::Created);

    // Fetch and validate pending account balances.
    let accounts = client
        .lookup_accounts(&[account_dr_id, account_cr_id])
        .await?;
    assert_eq!(accounts.len(), 2);

    for account in &accounts {
        if account.id == account_dr_id {
            assert_eq!(account.debits_posted, 0, "DR debits, before posted");
            assert_eq!(account.credits_posted, 0, "DR credits, before posted");
            assert_eq!(
                account.debits_pending, 500,
                "DR debits pending, before posted"
            );
            assert_eq!(
                account.credits_pending, 0,
                "DR credits pending, before posted"
            );
        } else if account.id == account_cr_id {
            assert_eq!(account.debits_posted, 0, "CR debits, before posted");
            assert_eq!(account.credits_posted, 0, "CR credits, before posted");
            assert_eq!(
                account.debits_pending, 0,
                "CR debits pending, before posted"
            );
            assert_eq!(
                account.credits_pending, 500,
                "CR credits pending, before posted"
            );
        } else {
            panic!("Unexpected account: {}", account.id);
        }
    }

    // Post pending transfer.
    let transfer_results = client
        .create_transfers(&[tb::Transfer {
            id: tb::id(),
            debit_account_id: account_dr_id,
            credit_account_id: account_cr_id,
            pending_id: transfer_pending_id,
            ledger: 1,
            code: 1,
            // You can post any amount between zero and the pending transfer amount,
            // or void the two-phase transfer using the `VoidPendingTransfer` flag.
            amount: 500,
            flags: tb::TransferFlags::PostPendingTransfer,
            ..Default::default()
        }])
        .await?;

    assert!(transfer_results.len() == 1);
    assert!(transfer_results[0].status == tb::CreateTransferStatus::Created);

    // Fetch and validate final account balances.
    let accounts = client
        .lookup_accounts(&[account_dr_id, account_cr_id])
        .await?;
    assert_eq!(accounts.len(), 2);

    for account in &accounts {
        if account.id == account_dr_id {
            assert_eq!(account.debits_posted, 500, "DR debits");
            assert_eq!(account.credits_posted, 0, "DR credits");
            assert_eq!(account.debits_pending, 0, "DR debits pending");
            assert_eq!(account.credits_pending, 0, "DR credits pending");
        } else if account.id == account_cr_id {
            assert_eq!(account.debits_posted, 0, "CR debits");
            assert_eq!(account.credits_posted, 500, "CR credits");
            assert_eq!(account.debits_pending, 0, "CR debits pending");
            assert_eq!(account.credits_pending, 0, "CR credits pending");
        } else {
            panic!("Unexpected account: {}", account.id);
        }
    }

    Ok(())
}

/// Minimal async executor for this sample. In a real application, use a proper async runtime.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct ThreadWaker(std::thread::Thread);

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
