const Docs = @import("../docs_types.zig").Docs;

pub const RubyDocs = Docs{
    .directory = "ruby",

    .markdown_name = "ruby",
    .extension = "rb",
    .proper_name = "Ruby",

    .test_source_path = "",

    .name = "tigerbeetle",
    .description =
    \\The TigerBeetle client for Ruby.
    \\
    \\>[!IMPORTANT]
    \\>This gem changed ownership from [Anthony D](https://github.com/antstorm)
    \\to TigerBeetle. If you're upgrading from a 0.0.x version, please consult
    \\the [migration guide](
    \\https://github.com/tigerbeetle/tigerbeetle/blob/main/src/clients/ruby/docs/migration.md
    \\) for the necessary code changes.
    ,
    .prerequisites =
    \\* Ruby >= `3.3`
    ,

    .project_file = "",
    .project_file_name = "",
    .test_file_name = "main",

    .install_commands = "gem install tigerbeetle",
    .run_commands = "ruby main.rb",

    .examples = "",

    .client_object_documentation =
    \\The gem also provides an optional top-level alias.
    \\Require `tigerbeetle/tb` to use `TB` as a shorthand for `TigerBeetle`:
    \\
    \\```ruby
    \\require "tigerbeetle/tb"
    \\
    \\account = TB::Account.new(id: TB.id, ledger: 1, code: 1)
    \\```
    \\
    \\The alias is opt-in and is not defined by `require "tigerbeetle"`.
    \\
    \\The `TigerBeetle::Client` is fiber-scheduler aware, so it works with e.g.
    \\the `async` gem without requiring code changes.
    \\
    \\```ruby
    \\require "async"
    \\require "async/semaphore"
    \\require "tigerbeetle"
    \\
    \\semaphore = Async::Semaphore.new(16)
    \\
    \\account_batches = Array.new(16) do
    \\  Array.new(1_000) do
    \\    TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 1)
    \\  end
    \\end
    \\
    \\TigerBeetle::Client.open(cluster_id: 0, replica_addresses: "3000") do |client|
    \\  Async do
    \\    account_batches
    \\      .map { |batch| semaphore.async { client.create_accounts(batch) } }
    \\      .each(&:wait)
    \\  end
    \\end
    \\```
    ,
    .create_accounts_documentation = "",
    .account_flags_documentation =
    \\To toggle behavior for an account, combine constants from the
    \\`TigerBeetle::AccountFlags` module with bitwise-or:
    \\
    \\* `AccountFlags::LINKED`
    \\* `AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS`
    \\* `AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS`
    \\* `AccountFlags::HISTORY`
    \\
    ,

    .create_accounts_errors_documentation =
    \\To handle errors you can compare the result status returned
    \\from `client.create_accounts` with constants in the
    \\`TigerBeetle::CreateAccountStatus` module.
    ,
    .create_transfers_documentation = "",
    .create_transfers_errors_documentation =
    \\To handle errors you can compare the result status returned
    \\from `client.create_transfers` with constants in the
    \\`TigerBeetle::CreateTransferStatus` module.
    ,

    .transfer_flags_documentation =
    \\To toggle behavior for a transfer, combine constants from the
    \\`TigerBeetle::TransferFlags` module with bitwise-or:
    \\
    \\* `TransferFlags::LINKED`
    \\* `TransferFlags::PENDING`
    \\* `TransferFlags::POST_PENDING_TRANSFER`
    \\* `TransferFlags::VOID_PENDING_TRANSFER`
    ,
};
