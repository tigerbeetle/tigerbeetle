---
sidebar_position: 2
---

# Hardware

TigerBeetle is designed to operate and provide more than adequate performance even on commodity
hardware.

## Storage

NVMe is preferred to SSD for high performance deployments.

However, spinning rust is perfectly acceptable, especially where a cluster is expected to be long
lived, and the data file is expected to be large. There is no requirement for NVMe or SSD.

A 20 TiB disk containing a replica's data file is enough to address on the order of 50 billion
accounts or transfers. It is more important to provision sufficient storage space for a replica’s
data file than to provision high performance storage. The data file is created before the server is
initially run and grows automatically.

A replica's data file may reside on local storage or else on remote storage. The most important
concern is to ensure [independent fault domains](./deploy.md#hardware-fault-tolerance) across
replicas.

The operator may consider the use of RAID 10 to reduce the need for remote recovery if a replica's
disk fails.

## Memory

ECC memory is recommended for production deployments.

A replica requires at least 6 GiB RAM per machine. Between 16 GiB and 32 GiB or more (depending on
budget) is recommended to be allocated to each replica for caching. TigerBeetle uses static
allocation and will use exactly how much memory is explicitly allocated to it for caching via
command line argument.

## CPU

TigerBeetle requires only a single core per replica machine. TigerBeetle at present [does not
utilize more cores](../about/performance.md#single-core-by-design), but may in future.

## Multitenancy

There are no restrictions on sharing a server with other tenant processes.
