# Recovering

If a replica's data file is permanently lost (for example, if the SSD fails) then a new data file
must be reformatted to restore the cluster.

The `tigerbeetle format` command must **not** be used for this purpose. The issue is that
`tigerbeetle format` would create a replica that believes that any operation that it hasn't seen can
be safely nack'd -- unaware of the promises it made which were lost with the old data file. This
could cause the cluster to lose committed data.

Instead of `tigerbeetle format`, use the `tigerbeetle recover` command (see below).

Note that `tigerbeetle recover` requires the cluster to be healthy and capable of view-changing.

Once `tigerbeetle recover` succeeds, run `tigerbeetle start` as normal. At this point, the new
replica will rejoin the cluster and state sync to repair itself.

## Example

```console
./tigerbeetle recover \
  --cluster=0 \
  --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 \
  --replica=2 \
  --replica-count=3 \
  ./0_2.tigerbeetle
```

(`--addresses` should include an address for the recovering replica, but it can be any address as it
is just a placeholder.)
