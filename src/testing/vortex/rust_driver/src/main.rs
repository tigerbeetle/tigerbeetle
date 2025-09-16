#![allow(unused)]
// This code reads better if all protocol byte conversions are transmutes -
// rustc would prefer us to use safe conversions for the u128s.

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

    while let Some(op) = input.receive()? {
        let result = execute(&mut client, op)?;
        output.send(result)?;
    }

    Ok(())
}

fn execute(client: &mut tb::Client, op: Request) -> AnyResult<Reply> {
    match op {
        Request::CreateAccounts(accounts) => {
            let response = client.create_accounts(&accounts);
            let response = block_on(response)?;
            Ok(Reply::CreateAccounts(response))
        }
        Request::CreateTransfers(transfers) => {
            let response = client.create_transfers(&transfers);
            let response = block_on(response)?;
            Ok(Reply::CreateTransfers(response))
        }
        Request::LookupAccounts(account_ids) => {
            let response = client.lookup_accounts(&account_ids);
            let response = block_on(response)?;
            Ok(Reply::LookupAccounts(response))
        }
        Request::LookupTransfers(transfer_ids) => {
            let response = client.lookup_transfers(&transfer_ids);
            let response = block_on(response)?;
            Ok(Reply::LookupTransfers(response))
        }
    }
}

impl CliArgs {
    fn parse(mut args: std::env::Args) -> AnyResult<CliArgs> {
        let _arg0 = args.next();
        let arg1 = args.next();
        let arg2 = args.next();
        let (arg1, arg2) = match (arg1, arg2) {
            (Some(arg1), Some(arg2)) => (arg1, arg2),
            _ => bail!("two arguments required"),
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

enum Request {
    CreateAccounts(Vec<tb::Account>),
    CreateTransfers(Vec<tb::Transfer>),
    LookupAccounts(Vec<u128>),
    LookupTransfers(Vec<u128>),
}

enum Reply {
    CreateAccounts(Vec<tb::CreateAccountsResult>),
    CreateTransfers(Vec<tb::CreateTransfersResult>),
    LookupAccounts(Vec<tb::Account>),
    LookupTransfers(Vec<tb::Transfer>),
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
    fn receive(&mut self) -> AnyResult<Option<Request>> {
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
                Ok(Some(Request::CreateAccounts(events)))
            }
            tbc::TB_OPERATION_TB_OPERATION_CREATE_TRANSFERS => {
                let mut events = Vec::with_capacity(event_count as usize);
                for i in 0..event_count {
                    let mut bytes = [0; mem::size_of::<tb::Account>()];
                    self.reader.read_exact(&mut bytes)?;
                    let event: tb::Transfer = unsafe { mem::transmute(bytes) };
                    events.push(event);
                }
                Ok(Some(Request::CreateTransfers(events)))
            }
            tbc::TB_OPERATION_TB_OPERATION_LOOKUP_ACCOUNTS => {
                let mut events = Vec::with_capacity(event_count as usize);
                for i in 0..event_count {
                    let mut bytes = [0; mem::size_of::<u128>()];
                    self.reader.read_exact(&mut bytes)?;
                    let event: u128 = unsafe { u128::from_ne_bytes(bytes) };
                    events.push(event);
                }
                Ok(Some(Request::LookupAccounts(events)))
            }
            tbc::TB_OPERATION_TB_OPERATION_LOOKUP_TRANSFERS => {
                let mut events = Vec::with_capacity(event_count as usize);
                for i in 0..event_count {
                    let mut bytes = [0; mem::size_of::<u128>()];
                    self.reader.read_exact(&mut bytes)?;
                    let event: u128 = unsafe { u128::from_ne_bytes(bytes) };
                    events.push(event);
                }
                Ok(Some(Request::LookupTransfers(events)))
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
    fn send(&mut self, result: Reply) -> AnyResult<()> {
        match result {
            Reply::CreateAccounts(results) => {
                let results_length = u32::try_from(results.len())?;
                self.writer.write_all(&results_length.to_le_bytes())?;
                for result in results {
                    let result = tbc::tb_create_accounts_result_t {
                        index: u32::try_from(result.index)?,
                        result: u32::from(result.result),
                    };
                    let bytes: [u8; mem::size_of::<tbc::tb_create_accounts_result_t>()] =
                        unsafe { mem::transmute(result) };
                    self.writer.write_all(&bytes)?;
                }
            }
            Reply::CreateTransfers(results) => {
                let results_length = u32::try_from(results.len())?;
                self.writer.write_all(&results_length.to_le_bytes())?;
                for result in results {
                    let result = tbc::tb_create_transfers_result_t {
                        index: u32::try_from(result.index)?,
                        result: u32::from(result.result),
                    };
                    let bytes: [u8; mem::size_of::<tbc::tb_create_transfers_result_t>()] =
                        unsafe { mem::transmute(result) };
                    self.writer.write_all(&bytes)?;
                }
            }
            Reply::LookupAccounts(results) => {
                let results_length = u32::try_from(results.len())?;
                self.writer.write_all(&results_length.to_le_bytes())?;
                for result in results {
                    let bytes: [u8; mem::size_of::<tb::Account>()] =
                        unsafe { mem::transmute(result) };
                    self.writer.write_all(&bytes)?;
                }
            }
            Reply::LookupTransfers(results) => {
                let results_length = u32::try_from(results.len())?;
                self.writer.write_all(&results_length.to_le_bytes())?;
                for result in results {
                    let bytes: [u8; mem::size_of::<tb::Transfer>()] =
                        unsafe { mem::transmute(result) };
                    self.writer.write_all(&bytes)?;
                }
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}
