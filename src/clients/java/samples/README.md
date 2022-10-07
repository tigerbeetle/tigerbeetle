# TigerBeetle Java examples

This folder contains the following examples:

### 1. [QuickStart.java](src/main/java/com/tigerbeetle/samples/QuickStart.java)

A quick overview of TigerBeetle's operation, creating two accounts and a single transfer between them.

How to run:

```bash
mvn exec:java@QuickStart 
```

Output:

```bash
Creating accounts ...
Creating transfer ...
Looking up accounts ...
╔════╤══════════╤═════════════════╤═══════════╤═══════╤══════════════╤═══════════════╤══════════════╤════════════════╗
║ ID │ UserData │ Code            │ Ledger    │ Flags │ DebitsPosted │ DebitsPending │ CreditPosted │ CreditsPending ║
╠════╪══════════╪═════════════════╪═══════════╪═══════╪══════════════╪═══════════════╪══════════════╪════════════════╣
║ 1  │ 0        │ 1001 - CUSTOMER │ 978 - EUR │ none  │ 9900         │ 0             │ 0            │ 0              ║
╟────┼──────────┼─────────────────┼───────────┼───────┼──────────────┼───────────────┼──────────────┼────────────────╢
║ 2  │ 0        │ 2001 - SUPPLIER │ 978 - EUR │ none  │ 0            │ 0             │ 9900         │ 0              ║
╚════╧══════════╧═════════════════╧═══════════╧═══════╧══════════════╧═══════════════╧══════════════╧════════════════╝
```
 
### 2. LinkedEvents.java

Demonstrates how to create groups of linked accounts and transfers that succeed or fail atomically.

How to run:

```bash
mvn run LinkedEvents
```

Output:

```bash
TODO ...
```

### 3. PendingTransfers.java

Demonstrates how pending transfers work: pending credits/debits, timeouts, and posting/voiding a transfer.

How to run:

```bash
mvn run PendingTransfers
```

Output:

```bash
TODO ...
```




