# Accounts

TigerBeetle uses fixed-size data structures to represent all data,
including accounts.

This means that sometimes you need to set temporary values for fields
that TigerBeetle, not you (the user), are responsible.

Account fields cannot be changed by the user after creation. However,
debits and credits fields are modified by TigerBeetle as transfers
happen to and from the account.

| Field           | Description                                                      | Size     | Set by      | Additional constraints                                                | Example                                                        |
|-----------------|------------------------------------------------------------------|----------|-------------|-----------------------------------------------------------------------|----------------------------------------------------------------|
| id              | Identifier for this account, defined by the user.                | 16 bytes | User        | Must be unique                                                        | An integer-encoded UUIDv4 or any other unique 128-bit integer. |
| user_data       | Secondary identifier to link this account to an external entity. | 16 bytes | User        | May be zero                                                           | An integer-encoded UUIDv4 or any other unique 128-bit integer. |
| reserved        | Internal-only field.                                             | 48 bytes | TigerBeetle | User must set to zero on write                                        |                                                                |
| ledger          | Identifier used to enforce transfers between the same ledger.    | 4 bytes  | User        | None                                                                  | `0` for USD and `1` for EUR.                                   |
| code            | Reason for the transfer.                                         | 2 bytes  | User        | None                                                                  | `0` for deposit, `1` for settlement.                           |
| flags           | Specifies behavior during transfers.                             | 2 bytes  | User        | See below for all flags                                               | `0b0100000000000000` to force debts not to exceed credits.     |
| debits_pending  | Amount of pending debits.                                        | 8 bytes  | TigerBeetle | User must set to zero on write, always a positive integer on read     | `500`                                                          |
| debits_posted   | Amount of non-pending debits.                                    | 8 bytes  | TigerBeetle | User must set to zero on write, always a positive integer on read     | `400`                                                          |
| credits_pending | Amount of pending credits.                                       | 8 bytes  | TigerBeetle | User must set to zero on write, always a positive integer on read     | `800`                                                          |
| credits_posted  | Amount of non-pending credits.                                   | 8 bytes  | TigerBeetle | User must set to zero on write, always a positive integer on read     | `200`                                                          |
| timestamp       | Time account was created.                                        | 8 bytes  | TigerBeetle | User must set to zero on write, UNIX timestamp in nanoseconds on read | `1662489240014463675`                                          |

## Account flags

The Account flags field is a bit field.
