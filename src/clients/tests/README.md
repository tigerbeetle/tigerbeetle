# Client Conformance Tests

- [Overview](#overview)
- [Test authoring](#test-authoring)
  - [Limits](#limits)
  - [Default client and test isolation](#default-client-and-test-isolation)
  - [Restrictions on bindings and values](#restrictions-on-bindings-and-values)
  - [Records](#records)
  - [Requirements](#requirements)
    - [Rules for using requirements](#rules-for-using-requirements)
    - [Emitter support](#emitter-support)
  - [Assertions](#assertions)
  - [Harness directives and arithmetic](#harness-directives-and-arithmetic)
- [Examples](#examples)

## Overview

- Conformance test suites are defined in `suites/*.zig`.
- They are never compiled, but parsed with `std.zig.Ast`.
- The file `conformance_test_api.zig` contains `@compileError` stubs for the test API. This avoids LSP warnings about undefined functions while editing tests.
- The parser (`conformance_test_parser.zig`) performs its own semantic checks against `vsr.tigerbeetle` types and the desired semantics of the test suite. It's conservative in its approach and leans towards failing.
- Each target language has an emitter in `src/clients/<lang>/<lang>_conformance.zig`. These generate the final conformance test suites, which are checked for freshness during CI runs.
- `conformance_test_list.zig` generates `conformance_tests.md` through `zig build conformance:generate`, and the client CI script (`src/scripts/ci.zig`) runs that step. This file links to every generated test.
- Certain tradeoffs were made to keep the parser simple. The goal was a generated conformance test suite, not a generic source-to-source transpiler.
- .NET, Java, and Python also generate an async copy of each case.

## Test authoring

- New suites need to be registered in the `suites` table in `conformance_test.zig`. This defines the order in the generated files, which are meant to read like a walkthrough. The second item of each entry is the name used in generated identifiers, for example `create_transfers`, not the file name `transfers_create`.
- Test descriptions become identifiers. Allowed characters: letters, digits, space, `_`, `-`, `,`, `'`, and `.`. Descriptions need to contain at least one alphanumeric character. They must stay unique after normalization, because the list scanner asserts on duplicates.

### Limits

`conformance_test_list.zig` defines two constants:

- `cases_max` is used to avoid unbounded allocation and is currently set to 32. This can be bumped up if we ever need a suite with more tests.
- `generated_line_length_max` is set to 150. While emitters try to avoid long lines, test descriptions can get fairly long and there's no meaningful way to make them shorter in languages like Java.

### Default client and test isolation

- `const client = ct.default_client;` at the top level registers that name as a client binding. The name can be anything. Without this line, calls on `client` fail with "unknown binding". This was added so `client` isn't just magically available in every test.
- The emitted name for the default client is always `client` (or `@client` in Ruby). It's recommended to use the same name in the Zig suites.
- The test harness uses a new client for every test, but all cases share one server. Isolation therefore depends on `ct.generate_id()`. Hard-coded ids can collide across cases. The only literal ids in the suites are `0` and out-of-range values, which test rejection.
- `create_client` cases (the `client_create` suite) do not use the default client.

### Restrictions on bindings and values

- Only `const`. No type annotations, because emitters pick the type and u128 needs special handling.
- Only `generate_id()`, `generate_ids(n)`, operation results, indexing, and field access can be bound. Literals, records, references, and `increment`/`decrement` cannot be bound as there was no need for that so far.
- Names must be unique within a case.
- Integer literals are kept as strings. That is how the parser expresses values above u128 and negative values for the `requires_unbounded_integers` cases.
- String literals cannot contain escape sequences.

### Records

- Records are anonymous struct literals standing in for the actual TigerBeetle types.
- Their types are inferred from the operation. For example, in `assert_equal`, write `.{…}`, not `Account{…}`. The parser rejects an explicit type.
- Flags can only be set to `true`. Omit a flag to leave it false.
- The parser treats every packed struct as flags (`Record.is_flags`).

### Requirements

Tests can optionally specify requirements. In case an emitter sets a requirement to `false`, an omission comment will be emitted instead of the test:

```go
// Omitted: "rejects a cluster_id above the maximum"
// Reason: requires unbounded integers
```

There are currently two defined requirements:

- `requires_unbounded_integers`: `true` for languages where integers outside the u128 range can be represented
- `requires_fractional_amounts`: `true` for languages where a float can be passed where an integer is expected

#### Rules for using requirements

- At most one requirement per case. A second requirement will fail to parse.
- The requirement must be the first statement in a test. If one is encountered later in a test, it will cause a parse error.
- All emitters exhaustively switch on the list of requirements. Adding a new requirement forces every emitter to make a decision about supporting it, which is intentional. Example from the Go emitter:
  ```zig
  fn satisfies(requirement: ast.Case.Requirement) bool {
      return switch (requirement) {
          .requires_unbounded_integers => false,
          .requires_fractional_amounts => false,
      };
  }
  ```

#### Emitter support

| language | unbounded integers | fractional amounts |
|----------|--------------------|--------------------|
| .NET     | no                 | no                 |
| Go       | no                 | no                 |
| Java     | no                 | no                 |
| Node     | yes                | no                 |
| Python   | yes                | yes                |
| Ruby     | yes                | yes                |

### Assertions

- `assert_id_ascending(ids)`: Asserts that a list of ids is in strictly ascending order.
- `assert_equal(results, .{…})` checks the exact length, but for each record it compares only the fields listed. An empty tuple is rejected. Use `assert_empty` instead.
- `assert_equal(a.x, b.y)` compares two fields and requires the same field type.
- `assert_greater_than` compares a field to an integer literal.
- `assert_fail_with` checks the error against the action:
  - `address_invalid`, `address_limit_exceeded`, `cluster_id_invalid` are valid only on `create_client`.
  - `client_closed` and `too_much_data` are valid only on client calls.
- `assert_fail` accepts any client error. Each emitter maps `ClientError` to its target language.

### Harness directives and arithmetic

- `increment`/`decrement` operate on u64 values (in practice, timestamps). The arguments are a binding and an integer literal. There's no support for arbitrary arithmetic expressions in order to keep the parser simple.
- `concurrently(n, call)`: `n` must be at least 2. It runs one thread per call on the shared client.
- `generate_ids(n)` sleeps 1 ms every `ids_between_sleeps` (10,000) ids, so that the batch covers several clock ticks for `assert_id_ascending`.
- `sleep_ms` should be quite self-explanatory.

## Examples

- Default client alias.
- Isolation through `ct.generate_id()`.
- Bindings of operation results, indexing, and field access.
- Records with implied types and flags.
- `sleep_ms` and `increment`.
- `assert_equal` on records, against an expression, and field to field.

```zig
// suites/transfers_import.zig
const ct = @import("../conformance_test_api.zig");
const client = ct.default_client;

test "imports a transfer with an explicit timestamp" {
    const reference_results = client.create_accounts(.{
        .{ .id = ct.generate_id(), .ledger = 1, .code = 1 },
    });
    const reference_result = reference_results[0];
    const reference = reference_result.timestamp;

    ct.sleep_ms(10);

    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{
            .id = debit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 1),
        },
        .{
            .id = credit_account_id,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 2),
        },
    });

    const transfer_id = ct.generate_id();
    const results = client.create_transfers(.{
        .{
            .id = transfer_id,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 100,
            .ledger = 1,
            .code = 1,
            .flags = .{ .imported = true },
            .timestamp = ct.increment(reference, 3),
        },
    });

    ct.assert_equal(results, .{.{ .status = .created }});

    const transfers = client.lookup_transfers(.{transfer_id});
    ct.assert_equal(transfers, .{
        .{ .id = transfer_id, .amount = 100, .flags = .{ .imported = true } },
    });

    const result = results[0];
    const transfer = transfers[0];
    ct.assert_equal(result.timestamp, ct.increment(reference, 3));
    ct.assert_equal(transfer.timestamp, result.timestamp);
}
```

- Requirement as the first statement.
- Omitted in .NET, Go, Java, and Node.
- `assert_fail` with any client error.

```zig
// suites/transfers_create.zig
const ct = @import("../conformance_test_api.zig");
const client = ct.default_client;

test "rejects a fractional amount" {
    ct.requires_fractional_amounts();

    const debit_account_id = ct.generate_id();
    const credit_account_id = ct.generate_id();
    client.create_accounts(.{
        .{ .id = debit_account_id, .ledger = 1, .code = 1 },
        .{ .id = credit_account_id, .ledger = 1, .code = 1 },
    });

    ct.assert_fail(client.create_transfers(.{
        .{
            .id = ct.generate_id(),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = 1.5,
            .ledger = 1,
            .code = 1,
        },
    }));
}
```
