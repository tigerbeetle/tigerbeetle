# Basic TigerBeetle Go Client Sample

This simple program creates two accounts in TigerBeetle and generates
1,000,000 transfers between them. Then it verifies that the sum of the
amount tranferred adds up according to the credit and debit balances
stored in TigerBeetle for each account.

To build and run:

```
$ go mod tidy
$ go build
$ ./basic
```
