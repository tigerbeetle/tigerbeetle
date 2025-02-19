# Coding

This section is aimed at programmers building applications on top of TigerBeetle. It is organized as
a series of loosely connected guides which can be read in any order.

- [System Architecture](./system-architecture.md) paints the big picture.
- [Data Modeling](./data-modeling.md) shows how to map business-level entities to the primitives
  provided by TigerBeetle.
- [Financial Accounting](./financial-accounting.md), a deep dive into double-entry bookkeeping.
- [Reliable Transfer Submission](./reliable-transaction-submission.md) explains the end-to-end
  principle and how it helps to avoid double spending.
- [Time](./time.md) lists the guarantees provided by the TigerBeetle cluster clock.
- [Two-Phase Transfers](./two-phase-transfers.md) introduces pending transfers, one of the most
  powerful primitives built into TigerBeetle.
- [Recipes](./recipes/) is a library of ready-made solutions for common business requirements such
  as a currency exchange.
- [Clients](./clients/) shows how to use TigerBeetle from the comfort of .Net, Go, Java, Node.js, or
  Python.

Subscribe to the [tracking issue #2231](https://github.com/tigerbeetle/tigerbeetle/issues/2231)
to receive notifications about breaking changes!
