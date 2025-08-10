# Coding

This section is aimed at programmers building applications on top of TigerBeetle. It is organized as
a series of loosely connected guides which can be read in any order.

- [System Architecture](./system-architecture.md) paints the big picture.
- [Data Modeling](./data-modeling.md) shows how to map business-level entities to the primitives
  provided by TigerBeetle.
- [Financial Accounting](./financial-accounting.md), a deep dive into double-entry bookkeeping.
- [Requests](./requests.md) outlines the database interface.
- [Reliable Transaction Submission](./reliable-transaction-submission.md) explains the end-to-end
  principle and how it helps to avoid double spending.
- [Two-Phase Transfers](./two-phase-transfers.md) introduces pending transfers, one of the most
  powerful primitives built into TigerBeetle.
- [Linked Events](./linked-events.md) shows how several transfers can be chained together into a
  larger transaction, which succeeds or fails atomically.
- [Time](./time.md) lists the guarantees provided by the TigerBeetle cluster clock.
- [Recipes](./recipes/) is a library of ready-made solutions for common business requirements such
  as a currency exchange.
- [Clients](./clients/) shows how to use TigerBeetle from the comfort of .NET, Go, Java, Node.js, or
  Python.

Subscribe to the [tracking issue #2231](https://github.com/tigerbeetle/tigerbeetle/issues/2231)
to receive notifications about breaking changes!
