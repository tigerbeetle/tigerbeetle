# `get_account_history`

Fetch the historical [`AccountBalance`](../account_balances.md)s of a given [`Account`](../accounts.md).

- Each balance returned has a corresponding transfer with the same
  [`timestamp`](../transfers.md#timestamp). See the
  [`get_account_transfers`](get_account_transfers.md) operation for more details.

- The amounts refer to the account balance recorded _after_ the transfer execution.

- Only accounts created with the flag [`history`](../accounts.md#flagshistory) set retain the
  history of balances.

## Event

The query filter. See [`AccountFilter`](../account_filter.md) for constraints.

## Result

- If the account has the flag [`history`](../accounts.md#flagshistory) set and any matching
  balances exist, return an array of [`AccountBalance`](../account_balances.md)s.  
- If the account does not have the flag [`history`](../accounts.md#flagshistory) set,
  return nothing.  
- If no matching balance exist, return nothing.  
- If any constraint is violated, return nothing. 

## Client libraries

For language-specific docs see:

* [.NET library](/src/clients/dotnet/README.md#get-account-history)
* [Java library](/src/clients/java/README.md#get-account-history)
* [Go library](/src/clients/go/README.md#get-account-history)
* [Node.js library](/src/clients/node/README.md#get-account-history)
