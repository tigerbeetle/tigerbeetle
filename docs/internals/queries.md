# Query execution.

In this document we explore the strategies to execute the most common query filters.

### Design.

The main goal of TigerBeetle's query engine is that **any query must be guaranteed to execute in
linear time relative to the number of [value blocks](#) that match the condition**.
This means that buffering the output of a scan, using intermediary sorts, hash joins, or doing
any post-filtering is not allowed during the query execution.

Obviously, this decision limits the complexity of queries TigerBeetle can execute.
For example, `ORDER BY` clauses other than `ORDER BY timestamp`, `GROUP BY`, `HAVING`, `SELECT`
lists, `LEFT|RIGHT JOIN`, and many other querying capabilities commonly found in OLAP and OLGP
databases are not available in TigerBeetle _by design_.

This is a fair trade-off, so _any_ query running during the hot path will execute at a predictable
time without disrupting the OLTP workload.

For more complex analytical queries, exporting data from TigerBeetle to an OLAP database is
highly recommended.

### Constraints.

- Conditions (e.g., `field = value`) must be evaluated by scanning an LSM Tree so that the `key`
  matches the condition.

  Only `id`, `timestamp`, and secondary indexes are part of the Tree's key.

  **Non-indexed fields are not allowed in the query criteria**.

- Merging multiple scans (e.g. intersection, union, and difference) must be done directly over the
  scan output (no output buffering).

  **All scans must yield values sorted in the same direction to produce mergeable outputs.**

  For the primary key _id_ and secondary indexes (which yields only the key suffix), it is only
  possible to get sorted results on exact-match scans (e.g. `field_a = 10`), and not on range
  operators such as `>`, `<`, `>=`, `<=`, or `between`.

- The query executor assumes that precedence and ordering of multiple query criteria are always
  expressed unambiguously by the query language.

  Examples:

  * The condition `(S₁ AND S₂) AND S₃` is invalid, and `S₁ AND S₂ AND S₃` is expected instead.

  * The condition `S₁ OR (S₂ OR S₃)` is invalid, and `S₁ OR S₂ OR S₃` is expected instead.

  * The condition `S₁ AND S₂ OR C` is invalid, and either `(S₁ AND S₂) OR S₃` or
    `S₁ AND (S₂ OR S₃)` is expected instead.

  Note: The parentheses shown in the examples are only illustrative and do not constitute part of
  the query language.

###  Merge algorithms

The **zig-zag merge join** algorithm performs the **intersection** of two scans by alternating
reading a join value in one and then looking it up in the other.
This efficiently moves the scans **skipping gaps**.

The zig-zag merge algorithm can also be modified to perform the **difference** of two scans,
but it's not suitable for performing the **union** of scans (since there's no need for skipping
values), for which the **k-way merge** algorithm is used.

###  Reference API used in the examples:

Here's the **oversimplified** API for scans.

Parameters and details that are irrelevant for the sake of the examples were omitted:

```zig
/// Searches for an exact match in the CompositeKey prefix.
/// Produces the criteria equivalent to
/// WHERE field = key_prefix.
fn scan_prefix(comptime field: FieldEnum, key_prefix: FieldType) *Scan;

/// Searches for a CompositeKey interval.
fn scan_range(
  comptime field: FieldEnum,
  key_min: CompositeKey,
  key_max: CompositeKey,
) *Scan;

/// Intersection of two scan S₁ ∩ S₂.
/// Produces the criteria equivalent to
/// WHERE <condition_1> AND <condition_2>.
fn merge_intersection(scans: []Scan) *Scan;

/// Union of two scans S₁ ∪ S₂.
/// Produces the criteria equivalent to
/// WHERE <condition_1> OR <condition_2>.
fn merge_union(scans: []Scan) *Scan;

/// Difference of two scans S₁ - S₂.
/// Produces the criteria equivalent to
/// WHERE <condition_1> AND NOT <condition_2>.
fn merge_difference(scan_a: *Scan, scan_b: *Scan) *Scan;
```
### 1. Secondary indexes

#### 1.1. Exact match by the prefix.

Scanning a secondary index (a.k.a. `IndexTree`) by its prefix executes a range query by the entire
composite key from `.{ .field=X, .timestamp=1}` to `{ .field=X, .timestamp=maxInt - 1}` sorted by
the tuple `(field, timestamp)`.

Input:
```sql
WHERE field_a = 100
```

Execution:
```zig
scan_prefix(.field_a, 100);
```

Equivalent execution:
```zig
scan_range(
  .field_a,
  &.{
    .field = 100,
    .timestamp = 1
  },
  &.{
    .field = 100,
    .timestamp = maxInt - 1
  },
);
```
---

#### 1.2. Multiple scans can be combined.

Exact matches by the prefix on `IndexTree`s produce results in the same sorting order
(the object's `timestamp`), so scans can be combined during the execution.

Input:
```sql
WHERE field_a = 1 AND field_b = 100
```

Execution:
```zig
merge_intersection(&.{
  scan_prefix(field_a, 1),
  scan_prefix(field_b, 100),
});
```
---

#### 1.3. Scans can also be combined at arbitrary nested levels.

Two scans combined will produce sorted results as well, which means that the results can be combined
again.

Input:
```sql
WHERE (field_a = 1 OR field_b = 100) AND field_c = 10
```

Execution:
```zig
merge_intersection(&.{
   merge_union(&.{
     scan_prefix(.field_a, 1),
     scan_prefix(.field_b, 100),
   }),
   scan_prefix(.field_c, 10),
);
```
---

#### 1.4. Multiple prefix values.

Scanning by multiple prefix values is a form of exact match, thus, they can be combined.

Input:
```sql
WHERE field_a IN (1,10,100) AND field_b = 1000
```

Equivalent input:
```sql
WHERE (field_a = 1 OR field_a = 10 OR field_a = 100) AND field_b = 1000
```

Execution:
```zig
merge_intersection(&.{
  merge_union(&.{
     scan_prefix(.field_a, 1),
     scan_prefix(.field_a, 10),
     scan_prefix(.field_a, 100),
  }),
  scan_prefix(.field_b, 1000),
});
```
---

### 2. Timestamp.

#### 2.1. Timestamp and secondary indexes.

Timestamp filters (equality or ranges) can be associated with the secondary index's suffix
condition.

Input:
```sql
WHERE field_a = 1 AND timestamp BETWEEN 2024-1-1 AND 2024-1-31
```

Execution:
```zig
scan_range(
  .field_a,
  &.{
    .field = 1,
    .timestamp = 2024-1-1
  },
  &.{
    .field = 1,
    .timestamp = 2024-1-31
  },
);
```
---

#### 2.2. Timestamp only.

Timestamp filters must be evaluated using the `ObjectTree` when not intersecting with a secondary
index scan.

Input:
```sql
WHERE timestamp>=2024-1-1
```

Execution:
```zig
scan_range(.timestamp, 2024-1-1, maxInt - 1);
```
---

#### 2.3. Timestamps may be associated with secondary indexes.

Timestamp filters must associate with secondary index scans they intersect with.

Input:
```sql
WHERE  (field_a = 1 OR field_b = 10) AND timestamp >= 2024-1-1
```

Execution:

```zig
merge_union(&.{
  scan_range(
    .field_a,
    &.{
      .field = 1,
      .timestamp = 2024-1-1
    },
    &.{
      .field = 1,
      .timestamp = maxInt-1,
    },
  ),
  scan_range(
    .field_b
    &.{
      .field = 10,
      .timestamp = 2024-1-1
    },
    &.{
      .field = 10,
      .timestamp = maxInt-1,
    },
  ),
});
```
---

#### 2.4. Timestamps may need to be isolated from secondary indexes.

Timestamp filters must not associate with secondary index scans on **unions**.

Input:
```sql
WHERE
  (field_a = 1 AND timestamp BETWEEN 2024-1-1 AND 2024-1-31) OR field_b = 10
```

Execution:
```zig
merge_union(&.{
  scan_range(
    .field_a,
    &.{
      .field = 1,
      .timestamp = 2024-1-1
    },
    &.{
      .field = 1,
      .timestamp = 2024-1-31
    },
  ),
  scan_prefix(.field_b, 10),
});
```
---

Since the `ObjectTree` is keyed by `timestamp`, even range filters (e.g. `>`, `<`, `>=`, `<=`, and
`between`) produce sorted results that might be joined with other scans.

```sql
WHERE field_a = 10 OR timestamp BETWEEN 2024-1-1 AND 2024-1-31
```

Execution:
```zig
merge_union(&.{
  scan_prefix(.field_a, 10),
  scan_range(.timestamp, 2024-1-1, 2024-1-31)
});
```
---

#### 2.5. Timestamps and primary keys.

Timestamp filters intersecting with the primary key must be evaluated using the `ObjectTree`.

```sql
WHERE id = 1 AND timestamp BETWEEN 2024-1-1 AND 2024-1-31
```

Execution:

Scans over the primary key `id` behave mostly like secondary indexes, except they don't have
a prefix/suffix to include the timestamp.

```zig
merge_intersection(&.{
  scan_prefix(.id, 1),
  scan_range(.timestamp, 2024-1-1, 2024-1-31)
});
```
---

#### 2.6. Everything together.

Complex cases might need some extra rules:

** Not all scans that intersect are secondary indexes:

Input:
```sql
WHERE
  (
    id = 1
    OR
    (field_a = 100 AND field_b = 1000)
  )
  AND
  timestamp BETWEEN 2024-1-1 AND 2024-1-31
```

Execution:

The field `id` isn't a secondary index, the timestamp needs to be evaluated from the `ObjectTree`
instead.

```zig
merge_intersection(&.{
  merge_union(&.{
    scan_prefix(.id, 1),
    merge_intersection(&.{
      scan_prefix(.field_a, 100),
      scan_prefix(.field_b, 1000),
    }),
  }),
  scan_prefix(.timestamp, 2024-1-1, 2024-1-31),
});
```
---

** The timestamp intersects only with secondary indexes with the same (or lower) precedence:

Input:
```sql
WHERE
  (
    field_a = 1
    OR
    (field_b = 100 AND field_c = 1000)
  )
  AND
  timestamp BETWEEN 2024-1-1 AND 2024-1-31
```

Execution:

The timestamp filter can associate with all the secondary indexes it intersects.
Note that there is no top `merge_intersection` with the timestamp like in the previous example.

```zig
merge_union(&.{
  scan_range(
    .field_a,
    &.{
      .field = 1,
      .timestamp = 2024-1-1
    },
    &.{
      .field = 1,
      .timestamp = 2024-1-31
    },
  ),
  merge_intersection(&.{
    scan_range(
      .field_b,
      &.{
        .field = 100,
        .timestamp = 2024-1-1
      },
      &.{
        .field = 100,
        .timestamp = 2024-1-31
      },
    ),
    scan_range(
      .field_c,
      &.{
        .field = 1000,
        .timestamp = 2024-1-1
      },
      &.{
        .field = 1000,
        .timestamp = 2024-1-31
      },
    ),
  }),
});
```
---

** Multiple timestamp filters:

Input:
```sql
WHERE
  (
    field_a = 1
    OR
    (field_b = 100 AND timestamp>=2024-2-1)
  )
  AND
  timestamp BETWEEN 2024-1-1 AND 2024-1-31
```

Execution:

The timestamp filter cannot associate with conditions that already have a timestamp filter
defined. In this case it needs to be evaluated from the `ObjectTree` instead.

Note that in this case, the inner and the outer timestamp filters are mutually exclusive, so the
entire criteria `(field_b = 100 AND timestamp>=2024-2-1)` should not output results when evaluated
together with `AND timestamp BETWEEN 2024-1-1 AND 2024-1-31`.

```zig
merge_intersection(&.{
  merge_union(&.{
    scan_prefix(.field_a, 1),
    scan_range(
      .field_b,
      &.{
        .field = 100,
        .timestamp = 2024-2-1
      },
      &.{
        .field = 100,
        .timestamp = maxInt - 1,
      },
    ),
  }),
  scan_prefix(.timestamp, 2024-1-1, 2024-1-31),
});
```
---

## 3. Inequalities

Given the condition `field_a <> 100`:

It could be expressed as: `field_a < 100 AND field_a > 100`.

However, in our case, it does not satisfy the [constraints](#constraints) for merging two scans
that don't have the same sorting order since the condition involves range operators.

A modified version of the zig-zag merge algorithm can be used to perform the **difference** of two
scans, which allows us to execute the equivalent inequality if it is expressed as:

```sql
AND NOT(field_a = 100)
```

The downside is that _difference_ always requires _two_ scans, it is sensitive to precedence (e.g.
`S₁ - S₂ - S₃` needs to be disambiguated either as `(S₁ - S₂) - S₃` or `S₁ - (S₂ - S₃)`), and it is
also sensitive to the order of the conditions (e.g. `S₁ - S₂` is not the same as `S₂ - S₁`), which
adds some extra complexity, rewriting inequalities in terms of differences.

#### 3.1. Inequalities in intersections.

Inequality in intersection with secondary indexes can be expressed as simple differences:

Input:
```sql
WHERE field_a = 10 AND field_b <> 100
```

Execution:

```zig
merge_difference(&.{
  scan_prefix(.field_a, 10),
  scan_prefix(.field_b, 100),
});
```
---

The same is true for primary keys as well:

Input:
```sql
WHERE id = 10 AND field_b <> 100
```

Execution:
```zig
merge_difference(&.{
  scan_prefix(.id, 10),
  scan_prefix(.field_b, 100),
});
```
---

#### 3.2. Inequality as a single condition.

The `ObjectTree` needs to be used as the first scan from which the difference will be calculated.

```sql
WHERE field_a <> 10
```

Execution:
```zig
merge_difference(&.{
  scan_range(.timestamp, 1, maxInt - 1),
  scan_prefix(.field_a, 10),
});
```
---

The same is true for primary keys as well:

```sql
WHERE id <> 10
```

Execution:
```zig
merge_difference(&.{
  scan_range(.timestamp, 1, maxInt - 1),
  scan_prefix(.id, 10),
});
```
---

#### 3.3. Multiple inequalities.

The `ObjectTree` needs to be used as the first scan, and all inequalities can be combined into
a single scan for the difference.

Input:
```sql
WHERE field_a <> 1 AND field_b <> 10 AND field_c <> 100
```

The equivalent difference can be expressed as:

```sql
WHERE NOT (field_a = 1 AND field_b = 10 AND field_c = 100)
```

Execution:
```zig
merge_difference(&.{
  scan_range(.timestamp, 1, maxInt - 1),
  merge_intersection(&.{
    scan_prefix(.field_a, 1),
    scan_prefix(.field_b, 10),
    scan_prefix(.field_c, 100),
  }),
});
```
---

The same is true for **unions** as well:

Input:
```sql
WHERE field_a <> 10 OR field_b <> 100 OR field_c <> 1000
```

The equivalent difference can be expressed as:

```sql
WHERE NOT (field_a = 10 OR field_b = 100 OR field_c = 1000)
```

Execution:
```zig
merge_difference(&.{
  scan_range(.timestamp, 1, maxInt - 1),
  merge_union(&.{
    scan_prefix(.field_a, 10),
    scan_prefix(.field_b, 100),
    scan_prefix(.field_c, 1000),
  }),
});
```
---

#### 3.4. Multiple inequalities and exact matches.

The exact match can be used as the first scan and the inequalities combined into a single scan
for the difference.

```sql
WHERE field_a <> 1 AND field_b <> 10 AND field_c = 100
```

The equivalent difference can be expressed as:

```sql
WHERE field_c = 100 AND NOT (field_a = 1 AND field_b = 10)
```

Execution:
```zig
merge_difference(&.{
  scan_prefix(.field_c, 100),
  merge_intersection(&.{
    scan_prefix(.field_a, 1),
    scan_prefix(.field_b, 10),
  }),
});
```
---

The same is true for **unions** as well:

```sql
WHERE (field_a <> 1 OR field_b <> 10) AND field_c = 100
```

The equivalent difference can be expressed as:

```sql
WHERE field_c = 100 AND NOT (field_a = 1 OR field_b = 10)
```

Execution:
```zig
merge_difference(&.{
  scan_prefix(.field_c, 100),
  merge_union(&.{
    scan_prefix(.field_a, 1),
    scan_prefix(.field_b, 10),
  }),
});
```
---

#### 3.5. Inequality with timestamps in intersection with secondary indexes.

The timestamp filter can communicate with secondary indexes in intersections and differences:

```sql
WHERE field_a <> 1 AND field_b = 10 AND timestamp >= 2024-2-1
```

Execution:
```zig
merge_difference(&.{
  scan_range(
    .field_b,
    &.{
      .field = 10,
      .timestamp = 2024-2-1
    },
    &.{
      .field = 10,
      .timestamp = maxInt - 1,
    },
  ),
  scan_range(
    .field_a,
    &.{
      .field = 1,
      .timestamp = 2024-2-1
    },
    &.{
      .field = 1,
      .timestamp = maxInt - 1,
    },
  ),
});
```
---

#### 3.6. Inequality and timestamps.

Timestamp ranges

In this case, the `ObjectTree` scan must be used as the first scan for the difference, and the
timestamp can be associated with both scans:

```sql
WHERE field_a <> 1 AND timestamp >= 2024-2-1
```

Execution:
```zig
merge_difference(&.{
  scan_range(.timestamp, 2024-2-1, maxInt - 1),
  scan_range(
    .field_a,
    &.{
      .field = 1,
      .timestamp = 2024-2-1
    },
    &.{
      .field = 1,
      .timestamp = maxInt - 1,
    },
  ),
});
```
---

#### 3.7. Inequality by timestamp.

Inequality by timestamp can be expressed with a range operation `timestamp < X AND timestamp > X`,
and can be associated with secondary indexes or directly from the `ObjectTree`.

```sql
WHERE field_a = 100 AND timestamp <> 2024-2-1
```

Execution:
```zig
merge_union(&.{
  scan_range(
    .field_a,
    &.{
      .field = 100,
      .timestamp = 1
    },
    &.{
      .field = 1,
      .timestamp = 2024-2-1 - 1,
    },
  ),
  scan_range(
    .field_a,
    &.{
      .field = 100,
      .timestamp = 2024-2-1 + ns_per_day
    },
    &.{
      .field = 1,
      .timestamp = maxInt - 1,
    },
  ),
});
```
---

```sql
WHERE timestamp <> 2024-2-1
```

Execution:
```zig
merge_union(&.{
  scan_range(.timestamp, 1, 2024-2-1 - 1),
  scan_range(.timestamp, 2024-2-1 + ns_per_day, maxInt - 1),
});
```
---
