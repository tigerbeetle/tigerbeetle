---
sidebar_position: 1
---

# Creating accounts and transfers in the Node CLI

Once you've got the TigerBeetle server running, let's connect to the
running server and do some accounting!

First install the Node client.

```javascript
$ npm install -g tigerbeetle-node
```

Then create a client connection.

```javascript
$ node
Welcome to Node.js v16.14.0.
Type ".help" for more information.
> let { createClient } = require('tigerbeetle-node');
> let client = createClient({ cluster_id: 0, replica_addresses: ['3000'] });
info(message_bus): connected to replica 0
```

Now create two accounts. (Don't worry about the details, you can
read about them later.)

```javascript
> let errors = await client.createAccounts([
  {
    id: 1n,
    ledger: 1,
    code: 718,
    user_data: 0n,
    reserved: Buffer.alloc(48, 0),
    flags: 0,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: 0n,
    timestamp: 0n,
  },
  {
    id: 2n,
    ledger: 1,
    code: 718,
    user_data: 0n,
    reserved: Buffer.alloc(48, 0),
    flags: 0,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: 0n,
    timestamp: 0n,
  },
]);
> errors
[]
```

Now create a transfer of `10` (of some amount/currency) between the two accounts.

```javascript
> errors = await client.createTransfers([
  {
    id: 1n,
    debit_account_id: 1n,
    credit_account_id: 2n,
    pending_id: 0n,
    user_data: 0n,
    reserved: 0n,
    timeout: 0n,
    ledger: 1,
    code: 718,
    flags: 0,
    amount: 10n,
    timestamp: 0n,
  }
]);
```

Now, the amount of `10` has been credited to account `2` and debited
from account `1`. Let's query TigerBeetle for these two accounts to
verify!

```javascript
> let accounts = await client.lookupAccounts([1n, 2n]);
> console.log(accounts.map(a => ({
    id: a.id,
	debits_posted: a.debits_posted,
	credits_posted: a.credits_posted,
	timestamp: a.timestamp,
  })));
[
  {
    id: 1n,
    debits_posted: 10n,
    credits_posted: 0n,
    timestamp: 1662489240014463675n
  },
  {
    id: 2n,
    debits_posted: 0n,
    credits_posted: 10n,
    timestamp: 1662489240014463676n
  }
]
```

And indeed you can see that account `1` has `debits_posted` as `10`
and account `2` has `credits_posted` as `10`. The `10` amount is fully
accounted for!
