# Fractional Amounts

To maximize precision and efficiency, [`Account`](../reference/accounts.md) debits/credits
and [`Transfer`](../reference/transfers.md) amounts are unsigned 128-bit integers.
However, currencies are often denominated in fractional amounts.

To represent a fractional amount in TigerBeetle, map the smallest useful unit of the fractional
currency to 1. Consider all amounts in TigerBeetle as a multiple of that unit.

Applications may rescale the integer amounts as necessary when rendering or interfacing with other
systems. But when working with fractional amounts, calculations should be performed on the integers
to avoid loss of precision due to e.g. floating-point approximations.

TigerBeetle stores information precisely and efficiently, while applications can still
present fractional amounts to their users in a way that they are familiar with seeing them.

### Asset Scale

When the multiplier is a power of 10 (e.g. `10 ^ n`), then the exponent `n` is referred to as an
_asset scale_. For example, representing USD in cents uses an asset scale of `2`.

## Examples

- In USD, `$1` = `100` cents. So for example,
  - The fractional amount `$0.45` is represented as the integer `45`.
  - The fractional amount `$123.00` is represented as the integer `12300`.
  - The fractional amount `$123.45` is represented as the integer `12345`.

## Oversized Amounts

The other direction works as well. If the smallest useful unit of a currency is `10,000,000 ¤`,
then it can be scaled down to the integer `1`.

The 128-bit representation defines the precision, but not the scale.
