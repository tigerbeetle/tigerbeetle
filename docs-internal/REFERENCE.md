# Data types

## Accounts

| Field           | Type        | Description                                                                                                                              | Example                                              |
| =====           | =========== | =======                                                                                                                                  | ====                                                 |
| id              | u128        | Primary key *specified by the user*. It is important for you to keep track of the id on your own so you can retry requests idempotently. | ee9f9417-cc28-4571-8f64-a0e615ab3462 (as byte array) |
| user_data       | u128        | An addition field for storing data related to this account. For example you might set it to the UUID of a group of accounts.             | ee9f9417-cc28-4571-8f64-a0e615ab3462 (as byte array) |
| reserved        | 48[u8]      | Internal-only field. Must always be set to 0.                                                                                            | 0                                                    |
| ledger          | u32         | TBD                                                                                                                                      | 1                                                    |
| code            | u16         | TBD                                                                                                                                      | 1                                                    |
| debits_pending  | u64         | TBD                                                                                                                                      | 0                                                    |
| debits_posted   | u64         | TBD                                                                                                                                      | 0                                                    |
| credits_pending | u64         | TBD                                                                                                                                      | 0                                                    |
| credits_posted  | u64         | TBD                                                                                                                                      | 0                                                    |
| timestamp       | u64         | Internal-only field. Must always be set to 0.                                                                                            | 0                                                    |

## Transfers


