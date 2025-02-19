# Requests

TigerBeetle supports the following request types:

- [`create_accounts`](./create_accounts.md): create [`Account`s](../account.md)
- [`create_transfers`](./create_transfers.md): create [`Transfer`s](../transfer.md)
- [`lookup_accounts`](./lookup_accounts.md): fetch `Account`s by `id`
- [`lookup_transfers`](./lookup_transfers.md): fetch `Transfer`s by `id`
- [`get_account_transfers`](./get_account_transfers.md): fetch `Transfer`s by `debit_account_id` or
  `credit_account_id`
- [`get_account_balances`](./get_account_balances.md): fetch the historical account balance by the
  `Account`'s `id`.
- [`query_accounts`](./query_accounts.md): query `Account`s
- [`query_transfers`](./query_transfers.md): query `Transfer`s

_More request types, including more powerful queries, are coming soon!_
