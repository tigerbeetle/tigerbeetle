# TigerBeetle Java examples

This folder contains the following examples:

## 1. [StartHere.java](src/main/java/com/tigerbeetle/samples/StartHere.java)

A basic overview of TigerBeetle's operation, creating two accounts and a single transfer between them.

How to run:

```bash
java ...
```

Output:

```bash
Account ID: 2a8c7db4-81d2-4f4c-a4ba-5ed88e3ff107
Code: 1001 - CUSTOMER
Ledger: 978 - EUR
Credits posted: 0
Debits posted: 9900
---------------------------------------------
Account ID: b41595c4-6616-406b-8d3e-6cd659cc73b0
Code: 2001 - SUPPLIER
Ledger: 978 - EUR
Credits posted: 9900
Debits posted: 0
---------------------------------------------
```
 
## 2. ResultHandling.java

Demonstrates how to handle the result codes returned from the `createAccounts` and `createTransfers` methods. 

How to run:

```bash
java ...
```

Output:

```bash
...
```

## 3. LinkedEvents.java

Demonstrates how to create groups of linked accounts and transfers that succeed or fail atomically.

How to run:

```bash
java ...
```

Output:

```bash
...
```

## 4. PendingTransfers.java

Demonstrates how pending transfers works: pending credits/debits, timeouts, and posting or voiding a pending transfer.

How to run:

```bash
java ...
```

Output:

```bash
...
```

## 5. DataTypes.java

Demonstrates how to work with TigerBeetle's data types `UInt128` and how to handle unsigned integers properly.




