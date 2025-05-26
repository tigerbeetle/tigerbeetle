#![allow(unused)]

use anyhow::Result as AnyResult;
use anyhow::{bail, Context};
use futures::executor::block_on;
use std::mem;
use std::str::FromStr;
use tb::tb_client as tbc;
use tigerbeetle as tb;

struct CliArgs {
    cluster_id: u128,
    addresses: String,
}

fn main() -> AnyResult<()> {
    let args = std::env::args();
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    let args = CliArgs::parse(args)?;
    let mut input = Input::from(stdin);
    let mut output = Output::from(stdout);

    let mut client = tb::Client::new(args.cluster_id, &args.addresses)?;

    loop {
        let Some(op) = input.receive()? else {
            break;
        };

        let result = execute(&mut client, op)?;

        output.send(result)?;
    }

    Ok(())
}

fn execute(client: &mut tb::Client, op: Operation) -> AnyResult<OperationResult> {
    match op {
        Operation::CreateAccounts(accounts) => {
            let response = client.create_accounts(&accounts);
            let response = block_on(response)?;
            Ok(OperationResult::CreateAccounts(response))
        }
        Operation::CreateTransfers(transfers) => {
            let response = client.create_transfers(&transfers);
            let response = block_on(response)?;
            Ok(OperationResult::CreateTransfers(response))
        }
        Operation::LookupAccounts(account_ids) => {
            let response = client.lookup_accounts(&account_ids);
            let response = block_on(response)?;
            Ok(OperationResult::LookupAccounts(response))
        }
        Operation::LookupTransfers(transfer_ids) => {
            let response = client.lookup_transfers(&transfer_ids);
            let response = block_on(response)?;
            Ok(OperationResult::LookupTransfers(response))
        }
    }
}

impl CliArgs {
    fn parse(mut args: std::env::Args) -> AnyResult<CliArgs> {
        let _arg0 = args.next();
        let arg1 = args.next();
        let arg2 = args.next();
        let (Some(arg1), Some(arg2)) = (arg1, arg2) else {
            bail!("two arguments required");
        };

        let cluster_id: u128 =
            u128::from_str(&arg1).context("cluster id (argument 1) must be u128")?;
        let addresses = arg2;

        Ok(CliArgs {
            cluster_id,
            addresses,
        })
    }
}

enum Operation {
    CreateAccounts(Vec<tb::Account>),
    CreateTransfers(Vec<tb::Transfer>),
    LookupAccounts(Vec<u128>),
    LookupTransfers(Vec<u128>),
}

enum OperationResult {
    CreateAccounts(Vec<tb::CreateAccountResult>),
    CreateTransfers(Vec<tb::CreateTransferResult>),
    LookupAccounts(Vec<Result<tb::Account, tb::NotFound>>),
    LookupTransfers(Vec<Result<tb::Transfer, tb::NotFound>>),
}

struct Input {
    reader: Box<dyn std::io::Read>,
}

impl From<std::io::Stdin> for Input {
    fn from(stdin: std::io::Stdin) -> Input {
        Input {
            reader: Box::new(stdin),
        }
    }
}

impl Input {
    fn receive(&mut self) -> AnyResult<Option<Operation>> {
        let op = {
            let mut bytes = [0; 1];
            if let Err(e) = self.reader.read_exact(&mut bytes) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(e.into());
                }
            }
            u8::from_le_bytes(bytes)
        };

        let event_count = {
            let mut bytes = [0; 4];
            self.reader.read_exact(&mut bytes)?;
            u32::from_le_bytes(bytes)
        };

        match op {
            tbc::TB_OPERATION_TB_OPERATION_CREATE_ACCOUNTS => {
                let mut events = Vec::with_capacity(event_count as usize);
                for i in 0..event_count {
                    let mut bytes = [0; mem::size_of::<tb::Account>()];
                    self.reader.read_exact(&mut bytes)?;
                    let event: tb::Account = unsafe { mem::transmute(bytes) };
                    events.push(event);
                }
                Ok(Some(Operation::CreateAccounts(events)))
            }
            tbc::TB_OPERATION_TB_OPERATION_CREATE_TRANSFERS => {
                let mut events = Vec::with_capacity(event_count as usize);
                for i in 0..event_count {
                    let mut bytes = [0; mem::size_of::<tb::Account>()];
                    self.reader.read_exact(&mut bytes)?;
                    let event: tb::Transfer = unsafe { mem::transmute(bytes) };
                    events.push(event);
                }
                Ok(Some(Operation::CreateTransfers(events)))
            }
            tbc::TB_OPERATION_TB_OPERATION_LOOKUP_ACCOUNTS => {
                let mut events = Vec::with_capacity(event_count as usize);
                for i in 0..event_count {
                    let mut bytes = [0; mem::size_of::<u128>()];
                    self.reader.read_exact(&mut bytes)?;
                    let event: u128 = unsafe { mem::transmute(bytes) };
                    events.push(event);
                }
                Ok(Some(Operation::LookupAccounts(events)))
            }
            tbc::TB_OPERATION_TB_OPERATION_LOOKUP_TRANSFERS => {
                let mut events = Vec::with_capacity(event_count as usize);
                for i in 0..event_count {
                    let mut bytes = [0; mem::size_of::<u128>()];
                    self.reader.read_exact(&mut bytes)?;
                    let event: u128 = unsafe { mem::transmute(bytes) };
                    events.push(event);
                }
                Ok(Some(Operation::LookupTransfers(events)))
            }
            _ => todo!("{op}"),
        }
    }
}

struct Output {
    writer: Box<dyn std::io::Write>,
}

impl From<std::io::Stdout> for Output {
    fn from(stdout: std::io::Stdout) -> Output {
        Output {
            writer: Box::new(stdout),
        }
    }
}

impl Output {
    fn send(&mut self, result: OperationResult) -> AnyResult<()> {
        match result {
            OperationResult::CreateAccounts(results) => {
                let results_len = u32::try_from(results.len())?;
                self.writer.write_all(&results_len.to_le_bytes())?;
                for (index, result) in results.into_iter().enumerate() {
                    let result = tbc::tb_create_accounts_result_t {
                        index: u32::try_from(index)?,
                        result: u32::from(result),
                    };
                    let bytes: [u8; mem::size_of::<tbc::tb_create_accounts_result_t>()] =
                        unsafe { mem::transmute(result) };
                    self.writer.write_all(&bytes)?;
                }
            }
            OperationResult::CreateTransfers(results) => {
                let results_len = u32::try_from(results.len())?;
                self.writer.write_all(&results_len.to_le_bytes())?;
                for (index, result) in results.into_iter().enumerate() {
                    let result = tbc::tb_create_transfers_result_t {
                        index: u32::try_from(index)?,
                        result: u32::from(result),
                    };
                    let bytes: [u8; mem::size_of::<tbc::tb_create_transfers_result_t>()] =
                        unsafe { mem::transmute(result) };
                    self.writer.write_all(&bytes)?;
                }
            }
            OperationResult::LookupAccounts(results) => {
                let results_len = u32::try_from(results.len())?;
                self.writer.write_all(&results_len.to_le_bytes())?;
                for result in results {
                    match result {
                        Ok(result) => {
                            let bytes: [u8; mem::size_of::<tb::Account>()] =
                                unsafe { mem::transmute(result) };
                            self.writer.write_all(&bytes)?;
                        }
                        Err(_) => { /* pass */ }
                    }
                }
            }
            OperationResult::LookupTransfers(results) => {
                let results_len = u32::try_from(results.len())?;
                self.writer.write_all(&results_len.to_le_bytes())?;
                for result in results {
                    match result {
                        Ok(result) => {
                            let bytes: [u8; mem::size_of::<tb::Transfer>()] =
                                unsafe { mem::transmute(result) };
                            self.writer.write_all(&bytes)?;
                        }
                        Err(_) => { /* pass */ }
                    }
                }
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}
