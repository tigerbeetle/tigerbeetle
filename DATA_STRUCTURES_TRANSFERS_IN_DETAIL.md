# Data Structures: Transfers In Detail

We will move to Option C in future.

```
Option A: transfer {
                id: 16 bytes (128-bit)
          payer_id:  6 bytes ( 48-bit)
          payee_id:  6 bytes ( 48-bit)
            amount:  6 bytes ( 48-bit)
  expire_timestamp:  6 bytes ( 48-bit)
  create_timestamp:  6 bytes ( 48-bit) [reserved]
  accept_timestamp:  6 bytes ( 48-bit) [reserved]
       userdata_id:  6 bytes ( 48-bit) [reserved]
} = 64 bytes (1 cpu cache line)
```

```
Option B: transfer {
                id: 16 bytes (128-bit)
          payer_id:  4 bytes ( 32-bit) (does not support random uuids, limits applications)
          payee_id:  4 bytes ( 32-bit) (does not support random uuids, limits applications)
            amount:  6 bytes ( 48-bit) (limits participant positions)
  expire_timestamp:  6 bytes ( 48-bit)
  create_timestamp:  6 bytes ( 48-bit) [reserved]
  accept_timestamp:  6 bytes ( 48-bit) [reserved]
          vector_0:  8 bytes ( 64-bit) (not enough entropy to be safe against collisions)
          vector_1:  8 bytes ( 64-bit) (not enough entropy to be safe against collisions)
} = 64 bytes (1 cpu cache line)
```

```
Option C: transfer {
                id: 16 bytes (128-bit)
          payer_id: 16 bytes (128-bit)
          payee_id: 16 bytes (128-bit)
       userdata_id: 16 bytes (128-bit)
            amount:  8 bytes ( 64-bit)
  expire_timestamp:  6 bytes ( 48-bit)
  create_timestamp:  6 bytes ( 48-bit) [reserved]
  accept_timestamp:  6 bytes ( 48-bit) [reserved]
  reject_timestamp:  6 bytes ( 48-bit) [reserved]
          vector_0: 16 bytes (128-bit) [reserved]
          vector_1: 16 bytes (128-bit) [reserved]
} = 128 bytes (2 cpu cache lines)
```

The user can insert a batch of userdata in advance if they want to.
When we see a non-zero userdata_id, we then do a hash table lookup to enforce the foreign key relation.
This can be used for ilp packets and conditions.
We would allow at most 1020 bytes per userdata entry, allowing for a 4 byte length prefix.
For users that don't need userdata, this keeps the transfer struct size small.
