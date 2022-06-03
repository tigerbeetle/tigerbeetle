# ProtoBeetle
ProtoBeetle is a performance sketch of the design components of TigerBeetle.

## Benchmark
This will:

* Install and build dependencies.
* Pre-allocate a contiguous 256 MB journal file.
* Create a million transfers for the benchmark.
* Run the benchmark!

```shell
git clone https://github.com/coilhq/tigerbeetle.git
cd tiger-beetle/demos/protobeetle
npm install
dd < /dev/zero bs=1048576 count=256 > journal
scripts/create-transfers
node server

# In another tab:
cd tiger-beetle/demos/protobeetle
time node stress
```

## Mojaloop
For integration with Mojaloop, we also created `fast-ml-api-adapter.js` to
transform individual Mojaloop HTTP transfer requests into batches and ship these
to ProtoBeetle across the network using `client.js`.
