---
sidebar_position: 4
---

# Creating accounts and transfers with the CLI client

Once you've got some TigerBeetle replicas running, let's connect to the
replicas and do some accounting!

First let's create two accounts. (Don't worry about the details, you
can read about them later.)

```console
./tigerbeetle client --cluster=0 --addresses=3000
```
```
TigerBeetle Client
  Hit enter after a semicolon to run a command.

Examples:
  create_accounts id=1 code=10 ledger=700,
                  id=2 code=10 ledger=700;
  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
  lookup_accounts id=1;
  lookup_accounts id=1, id=2;
```
```console
create_accounts id=1 code=10 ledger=700,
                id=2 code=10 ledger=700;
```
```console
info(message_bus): connected to replica 0
```

Now create a transfer of `10` (of some amount/currency) between the two accounts.

```console
create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
```

Now, the amount of `10` has been credited to account `2` and debited
from account `1`. Let's query TigerBeetle for these two accounts to
verify!

```console
lookup_accounts id=1, id=2;
```
```json
{
  "id": "1",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": [],
  "debits_pending": "0",
  "debits_posted": "10",
  "credits_pending": "0",
  "credits_posted": "0"
}
{
  "id": "2",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": "",
  "debits_pending": "0",
  "debits_posted": "0",
  "credits_pending": "0",
  "credits_posted": "10"
}
```

And indeed you can see that account `1` has `debits_posted` as `10`
and account `2` has `credits_posted` as `10`. The `10` amount is fully
accounted for!
