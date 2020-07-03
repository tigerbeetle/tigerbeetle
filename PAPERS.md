# Papers

## The collection of papers behind TigerBeetle:

* [LMAX - How to Do 100K TPS at Less than 1ms Latency - 2010](https://www.infoq.com/presentations/LMAX/) - Why a relational database is not the right solution. *#mechanical-sympathy*

* [The LMAX Exchange Architecture - High Throughput, Low Latency and Plain Old Java - 2014](https://skillsmatter.com/skillscasts/5247-the-lmax-exchange-architecture-high-throughput-low-latency-and-plain-old-java) - Sam Adams on the high-level design of LMAX.

* [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) - A high performance alternative to bounded queues for
exchanging data between concurrent threads.

* [Gray Failure](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf) - "The major availability breakdowns and performance anomalies we see in cloud environments tend to be caused by subtle underlying faults, i.e. gray failure rather than fail-stop failure."

* [The Tail at Store: A Revelation from Millions
of Hours of Disk and SSD Deployments](https://www.usenix.org/system/files/conference/fast16/fast16-papers-hao.pdf) - "We find that storage performance instability is not uncommon: 0.2% of the time, a disk is more than 2x slower than its peer drives in the same RAID group (and 0.6% for SSD). As a consequence, disk and SSD-based RAIDs experience at least one slow drive (i.e., storage tail) 1.5% and 2.2% of the time."

* [The Tail at Scale](https://www2.cs.duke.edu/courses/cps296.4/fall13/838-CloudPapers/dean_longtail.pdf) - "A simple way to curb latency variability is to issue the same request to multiple replicas and use the results from whichever replica responds first."

* [Werner Vogels on Amazon Aurora](https://www.allthingsdistributed.com/2019/03/Amazon-Aurora-design-cloud-native-relational-database.html) - *Bringing it all back home*... choice quotes:

  > Everything fails all the time. The larger the system, the larger the probability that something is broken somewhere: a network link, an SSD, an entire instance, or a software component.

  > And then, there's the truly insidious problem of "gray failures." These occur when components do not fail completely, but become slow. If the system design does not anticipate the lag, the slow cog can degrade the performance of the overall system.

  > ... you might have three physical writes to perform with a write quorum of 2. You don't have to wait for all three to complete before the logical write operation is declared a success. It's OK if one write fails, or is slow, because the overall operation outcome and latency aren't impacted by the outlier. This is a big deal: A write can be successful and fast even when something is broken.

* [Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases](https://www.allthingsdistributed.com/files/p1041-verbitski.pdf)

* [Amazon Aurora: On Avoiding Distributed Consensus for I/Os, Commits, and Membership Changes](https://dl.acm.org/doi/pdf/10.1145/3183713.3196937?download=true)

* [Amazon Aurora Under The Hood - Quorum and Correlated Failure](https://aws.amazon.com/blogs/database/amazon-aurora-under-the-hood-quorum-and-correlated-failure/)

* [Amazon Aurora Under The Hood - Quorum Reads and Mutating State](https://aws.amazon.com/blogs/database/amazon-aurora-under-the-hood-quorum-reads-and-mutating-state/)

* [Amazon Aurora Under The Hood - Reducing Costs using Quorum Sets](https://aws.amazon.com/blogs/database/amazon-aurora-under-the-hood-reducing-costs-using-quorum-sets/)

* [Amazon Aurora Under The Hood - Quorum Membership](https://aws.amazon.com/blogs/database/amazon-aurora-under-the-hood-quorum-membership/)

* [ZFS: The Last Word in File Systems (Jeff Bonwick and Bill Moore)](https://www.youtube.com/watch?v=NRoUC9P1PmA) - On disk failure and corruption, the need for checksums... and checksums to check the checksums, and the power of copy-on-write for crash-safety.
