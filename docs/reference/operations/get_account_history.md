# `get_account_history`

Fetch the historical [`AccountBalance`](../account_balance.md)s of a given [`Account`](../accounts.md).

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

* [Looking up transfers using the .NET library](/src/clients/dotnet/README.md#get-account-history)
* [Looking up transfers using the Java library](/src/clients/java/README.md#get-account-history)
* [Looking up transfers using the Go library](/src/clients/go/README.md#get-account-history)
* [Looking up transfers using the Node.js library](/src/clients/node/README.md#get-account-history)
