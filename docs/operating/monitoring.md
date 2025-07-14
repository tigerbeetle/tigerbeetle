# Monitoring

TigerBeetle supports emitting metrics via StatsD, and uses the
[DogStatsD format for tags.](https://docs.datadoghq.com/developers/dogstatsd/datagram_shell?tab=metrics)

This requires a StatsD compatible agent running locally. The Datadog Agent works out of the
box with its default configuration, as does Telegraf's [statsd plugin](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/statsd/README.md),
with `datadog_extensions` enabled.

You can enable emitting metrics by adding the following CLI flags to each replica, depending on your
[deployment method](./deploying/):

```
--experimental --statsd=127.0.0.1:8125
```

The `--statsd` argument must be specified as an `IP:Port` address (IPv4 or IPv6).  DNS names are not
currently supported.

All TigerBeetle metrics are namespaced under `tb.` and are tagged with `cluster` (the cluster ID
specified at format time) and `replica` (the replica index). Specific metrics might have additional
tags - you can see a full list of metrics and cardinality by running `tigerbeetle inspect metrics`.

## Specific Metrics

### Overall status
The `replica_status` metric corresponds to the overall status of the replica. If it's anything other
than 0, it should be alerted on as it indicates a non-normal status. The full values are:

| Value | Status          | Explanation                                                                                                                                    |
|-------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| 0     | normal          | The replica is functioning normally.                                                                                                           |
| 1     | view_change     | The replica is doing a view change.                                                                                                            |
| 2     | recovering      | The replica is recovering. Usually, this will be present on startup before immediately transitioning to normal.                                |
| 3     | recovering_head | The replica's persistent state is corrupted, and it can't participate in consensus. It will try and recover from the remainder of the cluster. |

### State sync status
The `replica_sync_stage` metric corresponds to the state sync stage. If this is anything other than
`0`, the replica is undergoing state sync and should be alerted on.

### Operations timing
The `replica_request` timing metric can help inform how long requests are taking. This is tagged
with the operation type (e.g., `create_accounts`) and is the closest measure of how long a request
takes end to end, from the replica's point of view.

It's recommended to additionally add metrics around your TigerBeetle client code, to measure the
full request latency, including things like network delay which aren't captured here.

### Cache monitoring and sizing
The `grid_cache_hits` and `grid_cache_misses` metrics can help inform if your grid cache
(`--cache-grid`) is sized too small for your workload.

## System Monitoring
In addition to TigerBeetle's own metrics, it's recommended to monitor and alert on a few additional
system level metrics. These are:

* Disk space used, on the path that has the TigerBeetle data file.
* NTP clock sync status.
* Memory utilization: once started, TigerBeetle will use a fixed amount of memory and not change. A
  change in memory utilization can indicate a problem with other processes on the server.
* CPU utilization: TigerBeetle will use at most a single core at present. CPU utilization exceeding
  a single core can indicate a problem with other processes on the server.

While a specific alerting threshold is hard to define for the following, they are useful to monitor
to help diagnose problems:

* Network bandwidth utilization.
* Disk bandwidth utilization.
