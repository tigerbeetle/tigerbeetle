# Data Structures

## Participant

```
participant {
                id:  8 bytes ( 64-bit)
     net_debit_cap:  8 bytes ( 64-bit)
          position:  8 bytes ( 64-bit)
} = 24 bytes (1 cpu cache line)
```

## Transfer

```
transfer {
                id: 16 bytes (128-bit)
          payer_id:  8 bytes ( 64-bit)
          payee_id:  8 bytes ( 64-bit)
            amount:  8 bytes ( 64-bit)
  expire timestamp:  6 bytes ( 48-bit)
  create timestamp:  6 bytes ( 48-bit) [reserved]
  commit timestamp:  6 bytes ( 48-bit) [reserved]
       userdata_id:  6 bytes ( 48-bit) [reserved]
} = 64 bytes (1 cpu cache line)
```

We can expand the above transfer structure as needed in the production version,
or as needed by the requirements.

**This is a first stab for getting raw load test numbers.**

Rationale for being tightly packed:

* Aligning to 64-byte cpu cache lines reduces memory bandwidth.
* If we go from 1 to 2 cache lines by adding fields then we waste 50% L1 cache.
* The smaller the transfer, the more transfers fit in a 1220-byte network
packet.

Do we want to introduce another structure for a fulfil? This brings complexity.
We then need more code for managing two different structures, separate lookup
tables etc.

If we leverage the same data structure and fill out empty fields as the transfer
is fulfilled, then we can keep the cache hot. For example, updating a hash table
by reference is faster than inserting a new entry into another hash table,
because the cost of the first insert can be amortized and because the entry is
already in cache.

We want the payer to give us the data in the same format as above so that we can
just check and process without having to allocate another structure, and without
having to do another memory copy beyond that already done by the kernel passing
the packet into user space. We can then pass the same structure on to the payee.
Look mom, no memory copies!

If we do one thing really well, then it would be nice to have only one structure
competing in our performance arena. Of course, we will need a few bookkeeping
structures (e.g. payers, payees).

