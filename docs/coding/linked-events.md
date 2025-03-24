# Linked Events

Events within a request [succeed or fail](../reference/requests/create_transfers.md#result)
independently unless they are explicitly linked using `flags.linked`
([`Account.flags.linked`](../reference/account.md#flagslinked) or
[`Transfer.flags.linked`](../reference/transfer.md#flagslinked)).

When the `linked` flag is specified, it links the outcome of a Transfer or Account creation with the
outcome of the next one in the request. These chains of events will all succeed or fail together.

**The last event in a chain is denoted by the first Transfer or Account without this flag.**

The last Transfer or Account in a request may never have the `flags.linked` set, as it would leave a
chain open-ended. Attempting to do so will result in the
[`linked_event_chain_open`](../reference/requests/create_transfers.md#linked_event_chain_open) error.

Multiple chains of events may coexist within a request to succeed or fail independently.

Events within a chain are executed in order, or are rolled back on error, so that the effect of each
event in the chain is visible to the next. Each chain is either visible or invisible as a unit to
subsequent transfers after the chain. The event that was the first to fail within a chain will have
a unique error result. Other events in the chain will have their error result set to
[`linked_event_failed`](../reference/requests/create_transfers.md#linked_event_failed).

### Linked Transfers Example

Consider this set of Transfers as part of a request:

| Transfer | Index in Request | flags.linked |
| -------- | ---------------- | ------------ |
| `A`      | `0`              | `false`      |
| `B`      | `1`              | `true`       |
| `C`      | `2`              | `true`       |
| `D`      | `3`              | `false`      |
| `E`      | `4`              | `false`      |

If any of transfers `B`, `C`, or `D` fail (for example, due to
[`exceeds_credits`](../reference/requests/create_transfers.md#exceeds_credits)), then `B`, `C`,
and `D` will all fail. They are linked.

Transfers `A` and `E` fail or succeed independently of `B`, `C`, `D`, and each other.

After the chain of linked events has executed, the fact that they were linked will not be saved. To
save the association between Transfers or Accounts, it must be
[encoded into the data model](./data-modeling.md), for example by adding an ID to one of
the [user data](./data-modeling.md#user_data) fields.
