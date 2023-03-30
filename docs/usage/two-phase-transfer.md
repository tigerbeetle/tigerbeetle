# Two-phase Transfer

The name "two-phase transfer" is a reference to the [two-phase commit
protocol for distributed
transactions](https://en.wikipedia.org/wiki/Two-phase_commit_protocol).

Single-phase transfers post funds to accounts immediately when they
are created. That is, they increase the
[`debits_posted`](../reference/accounts.md#debits_posted) and
[`credits_posted`](../reference/accounts.md#credits_posted) fields on
respective accounts.

In contrast to single-phase transfers, a two-phase transfer moves
funds in stages:

1. First, the pending transfer reserves funds. While reserved, they
cannot be used by either the payer or payee. Only the
[`debits_pending`](../reference/accounts.md#debits_pending) and
[`credits_pending`](../reference/accounts.md#credits_pending) fields are
increased on the relevant accounts at this point.

2. Later, the application creates another transfer — with either of
   the following flags:
* [`post_pending_transfer`](../reference/transfers.md#flagspost_pending_transfer): Move all (or part) of the reserved funds to the pending transfer's destination.
* [`void_pending_transfer`](../reference/transfers.md#flagsvoid_pending_transfer): Revert all of the reserved funds to the original account.

A pending transfer can only be posted or voided once. It cannot be
posted twice or voided then posted, etc.

When the pending transfer is resolved (posted or voided), the
[`debits_pending`](../reference/accounts.md#debits_pending) and
[`credits_pending`](../reference/accounts.md#credits_pending) fields
on the respective accounts are decreased by the
[`amount`](../reference/transfers.md#amount) of the **pending** transfer.

When the pending transfer is posted,
[`debits_posted`](../reference/accounts.md#debits_posted) and
[`credits_posted`](../reference/accounts.md#credits_posted) fields on
the respective accounts are increased by the **posting** transfer's
[`amount`](../reference/transfers.md#amount) (which cannot exceed the
pending amount, but need not equal the pending amount either).

When the pending transfer is voided,
[`debits_posted`](../reference/accounts.md#debits_posted) and
[`credits_posted`](../reference/accounts.md#credits_posted) are not
modified.

## Posting a subset of pending `amount`

Although an initial [`amount`](../reference/transfers.md#amount) is
reserved when a pending transfer is created, you can set the
[`amount`](../reference/transfers.md#amount) field to that amount *or*
smaller than that initial amount when posting a pending transfer.

In the event that you post less than the amount you initially
reserved, the rest of the amount not posted reverts back to the
original account.

## Timeouts

If a pending transfer is created with a timeout (which is optional),
then if it has not been posted or voided by the time the timeout
expires, the full amount will be voided.

## `credits_must_not_exceed_debits` and `debits_must_not_exceed_credits`

The pending transfer's amount is reserved in a way that the second
step in a two-phase transfer will never cause the accounts' configured
balance invariants
([`credits_must_not_exceed_debits`](../reference/accounts.md#flagscredits_must_not_exceed_debits)
or
[`debits_must_not_exceed_credits`](../reference/accounts.md#flagsdebits_must_not_exceed_credits))
to be broken, whether the second step is a post or void.

### Pessimistic pending transfers

If an account with
[`debits_must_not_exceed_credits`](../reference/accounts.md#flagsdebits_must_not_exceed_credits)
has `credits_posted = 100` and `debits_posted = 70` and a pending
transfer is started causing the account to have `debits_pending = 50`,
the *pending* transfer will fail. It will not wait to get to *posted*
status to fail.

## All transfers are immutable

To reiterate, completing a two-phase transfer (by either marking it
void or posted) does not involve modifying the pending
transfer. Instead you create a new transfer.

The first transfer that is marked pending will always have its pending
flag set.

The second transfer will have a
[`post_pending_transfer`](../reference/transfers.md#flagspost_pending_transfer)
or
[`void_pending_transfer`](../reference/transfers.md#flagsvoid_pending_transfer2)
flag set and a [`pending_id`](../reference/transfers.md#pending_id) field
set to the [`id`](../reference/transfers.md#id) of the first transfer. The
[`id`](../reference/transfers.md#id) of the second transfer will be
unique, not the same [`id`](../reference/transfers.md#id) as the initial
pending transfer.

## Client documentation

Read more about how two-phase transfers work with each client.

* [Node](/src/clients/node/README.md#two-phase-transfers)
* [Go](/src/clients/go/README.md#two-phase-transfers)
* [Java](/src/clients/java/README.md#two-phase-transfers)

## Client samples

Or take a look at how it works in practice.

* [Node](/src/clients/node/samples/two-phase/main.js)
* [Go](/src/clients/go/samples/two-phase/main.go)
* [Java](/src/clients/java/samples/two-phase/Main.java)
