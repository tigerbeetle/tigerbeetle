use std::{
    error::Error,
    future::Future,
    rc::Rc,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use tigerbeetle as tb;

fn main() -> Result<(), Box<dyn Error>> {
    let cluster_id = 0;
    let tigerbeetle_port = std::env::var("TB_ADDRESS").unwrap_or_else(|_| "3000".to_string());
    let tigerbeetle_client = Arc::new(tb::Client::new(cluster_id, &tigerbeetle_port)?);

    let batch_size: u32 = getenv_or_default("TB_BATCH_SIZE", "128")?;
    let thread_count: u32 = getenv_or_default("TB_THREADS", "2")?;
    let thread_tasks: u32 = getenv_or_default("TB_TASKS", "64")?;
    let operation_pool_size: u32 = getenv_or_default("TB_POOL_SIZE", "64")?;
    let transfer_count_target: u64 = getenv_or_default("TB_TRANSFERS", "5000")?;

    // Create the necessary accounts for the benchmark.
    let account_count_min: u32 = 2;
    let account_count = block_on(create_accounts(
        &tigerbeetle_client,
        batch_size.max(account_count_min),
    ))?;
    assert!(account_count >= account_count_min);

    // Spawn threads with workers that create transfers.
    let t_start = std::time::Instant::now();
    let transfer_count = Arc::new(AtomicI64::new(transfer_count_target as i64));
    let threads: Vec<_> = (0..thread_count)
        .map(|thread_id| {
            let client = tigerbeetle_client.clone();
            let transfer_count = transfer_count.clone();
            std::thread::spawn(move || {
                // We use one request pool per thread here, but could also create a single
                // global pool instead at the cost of more cross-thread synchronization.
                let operation_pool = Rc::new(OperationPool::new(operation_pool_size, batch_size));
                block_on(thread(ThreadParameters {
                    client,
                    operation_pool,
                    transfer_count,
                    account_count,
                    thread_id,
                    thread_tasks,
                    batch_size,
                }))
            })
        })
        .collect();

    let mut processed_global: u64 = 0;
    for thread in threads {
        match thread.join() {
            Ok(Ok(processed_thread)) => processed_global += processed_thread,
            Ok(Err(error)) => return Err(format!("thread execution error: {}", error).into()),
            Err(_join_error) => return Err("thread join error".into()),
        }
    }

    assert_eq!(processed_global, transfer_count_target);
    assert!(transfer_count.load(Ordering::SeqCst) <= 0);

    let elapsed = t_start.elapsed();
    println!(
        "elapsed: {}ms | transfers: {} | tps: {} ",
        elapsed.as_millis(),
        processed_global,
        processed_global as f64 / elapsed.as_secs_f64(),
    );

    Ok(())
}

async fn create_accounts(client: &tb::Client, batch_size: u32) -> Result<u32, Box<dyn Error>> {
    let account_count: usize = batch_size as usize;
    let mut accounts: Vec<tb::Account> = (0..account_count)
        .map(|n| tb::Account {
            // these must not be 0
            id: 1 + (n as u128),
            ledger: 1,
            code: 1,
            // we link all account creation requests, so that they all
            // succeed or fail together and we never have to roll back
            flags: tb::AccountFlags::Linked,
            ..Default::default()
        })
        .collect();

    // the last request in the chain must not have the linked flag set
    accounts[account_count - 1].flags = tb::AccountFlags::empty();

    let completion = tb::Completion::new();

    // Reusable methods return completion state and buffers for the next request.
    // `client.create_accounts(...)` is more convenient but always allocates.
    let (_completion, _accounts, results) = client
        .create_accounts_reusable(completion, accounts, Vec::with_capacity(account_count))
        .await?;

    let mut failed_links = 0;
    for result in &results {
        match result.status {
            tb::CreateAccountStatus::Created => {}
            tb::CreateAccountStatus::LinkedEventFailed => failed_links += 1,
            status => return Err(format!("error creating accounts: {}", status).into()),
        }
    }

    assert_eq!(results.len(), account_count);
    assert_eq!(0, failed_links);
    Ok(account_count as u32)
}

struct ReusableRequest {
    completion: tb::Completion,
    source: Vec<tb::Transfer>,
    target: Vec<tb::CreateTransferResult>,
}

// The operation pool keeps reusable completions and input/output `Vec`s.
// Workers acquire a completion and buffers, fill the input vector, and submit the request.
struct OperationPool {
    free: std::sync::Mutex<Vec<ReusableRequest>>,
    wake: tokio::sync::Notify,
}

impl OperationPool {
    fn new(buffer_count: u32, batch_size: u32) -> OperationPool {
        Self {
            free: std::sync::Mutex::new(
                (0..buffer_count)
                    .map(|_| ReusableRequest {
                        completion: tb::Completion::new(),
                        source: Vec::with_capacity(batch_size as usize),
                        target: Vec::with_capacity(batch_size as usize),
                    })
                    .collect(),
            ),
            wake: tokio::sync::Notify::new(),
        }
    }

    async fn pop(&self) -> ReusableRequest {
        for _ in 0..1000 {
            if let Some(request) = self.try_pop() {
                return request;
            }
            self.wake.notified().await;
        }
        panic!("safety counter exceeded: couldn't acquire operation from pool, likely too small");
    }

    fn try_pop(&self) -> Option<ReusableRequest> {
        let mut free = self.free.lock().unwrap();
        free.pop()
    }

    fn reuse(
        &self,
        completion: tb::Completion,
        mut source: Vec<tb::Transfer>,
        mut output: Vec<tb::CreateTransferResult>,
    ) {
        source.clear();
        output.clear();
        {
            let mut free = self.free.lock().unwrap();
            free.push(ReusableRequest {
                completion,
                source,
                target: output,
            })
        }

        self.wake.notify_one();
    }
}

impl Drop for OperationPool {
    fn drop(&mut self) {
        let free = self.free.lock().unwrap();
        assert!(free.capacity() == free.len());
    }
}

fn acquire_batch(transfer_count: &AtomicI64, batch_size: i64) -> Option<u64> {
    let transfers_left = transfer_count.fetch_sub(batch_size, Ordering::Acquire);
    match transfers_left.min(batch_size) {
        transfers if transfers > 0 => Some(transfers as u64),
        _ => None,
    }
}

struct ThreadParameters {
    client: Arc<tb::Client>,
    operation_pool: Rc<OperationPool>,
    transfer_count: Arc<AtomicI64>,
    account_count: u32,
    thread_id: u32,
    thread_tasks: u32,
    batch_size: u32,
}

async fn thread(cfg: ThreadParameters) -> Result<u64, String> {
    // A worker submits batches while there are transfers to work on and reusable completions
    // and Vecs in its thread-local operation pool. The worker does not allocate per request.
    let worker = |task_id: u32| {
        let unique_task_id = (cfg.thread_tasks * cfg.thread_id) + task_id;
        let mut rng_state = unique_task_id as u64;
        let operation_pool = cfg.operation_pool.clone();
        let transfer_count = cfg.transfer_count.clone();
        async move {
            let transfer_count = transfer_count.as_ref();
            let mut processed: u64 = 0;
            while let Some(transfer_count) = acquire_batch(transfer_count, cfg.batch_size as i64) {
                let mut reusable = operation_pool.pop().await;

                assert!(reusable.source.is_empty());
                assert!(reusable.target.is_empty());
                assert!(reusable.source.capacity() as u64 >= transfer_count);
                assert!(reusable.target.capacity() as u64 >= transfer_count);

                // generate random transfers
                for id in 0..transfer_count {
                    let account_debit = 1 + (rng(&mut rng_state) % (cfg.account_count as u64));
                    let account_credit = 1 + (account_debit % (cfg.account_count as u64));
                    let amount = rng(&mut rng_state) % 100_000;
                    reusable.source.push(tb::Transfer {
                        id: tb::id(),
                        debit_account_id: account_debit.into(),
                        credit_account_id: account_credit.into(),
                        amount: amount.into(),
                        ledger: 1,
                        code: 1,
                        user_data_64: id + processed,
                        user_data_32: unique_task_id,
                        ..Default::default()
                    });
                }

                let ReusableRequest {
                    completion,
                    source,
                    target,
                } = reusable;
                let result = cfg
                    .client
                    .create_transfers_reusable(completion, source, target)
                    .await;

                match result {
                    Ok((reusable, transfers, create_transfer_results)) => {
                        for create_transfer_result in create_transfer_results.iter() {
                            assert_eq!(
                                create_transfer_result.status,
                                tb::CreateTransferStatus::Created
                            );
                        }
                        operation_pool.reuse(reusable, transfers, create_transfer_results);
                    }
                    Err(error) => return Err(error),
                };

                processed += transfer_count;
            }
            Ok(processed)
        }
    };

    let task_set = tokio::task::LocalSet::new();
    task_set
        .run_until(async {
            let tasks: Vec<_> = (0..cfg.thread_tasks)
                .map(|task_id| task_set.spawn_local(worker.clone()(task_id)))
                .collect();

            let mut processed_thread = 0;
            for task in tasks {
                match task.await {
                    Ok(Ok(processed_task)) => processed_thread += processed_task,
                    Ok(Err(task_error)) => {
                        return Err(format!("task returned with error: {}", task_error))
                    }
                    Err(join_error) => {
                        return Err(format!("error while joining task: {}", join_error))
                    }
                }
            }
            Ok(processed_thread)
        })
        .await
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn rng(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn getenv_or_default<T: std::str::FromStr>(var_name: &str, default: &str) -> Result<T, T::Err> {
    std::env::var(var_name)
        .unwrap_or_else(|_| default.to_string())
        .parse()
}
