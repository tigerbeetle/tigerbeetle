# TigerBeetle Changelog

## 2023-11-20

### Safety And Performance

- [#1300](https://github.com/tigerbeetle/tigerbeetle/pull/1300)

  Recursively check for padding in structs used for data serialization, ensuring that no uninitialized bytes can be stored or transmitted over the network. Previously, we checked only if the struct had no padding, but not its fields.

### Internals  

- [#1299](https://github.com/tigerbeetle/tigerbeetle/pull/1299)

  Minor adjustments in the release process, making it easier to track updates in the documentation website when a new version is released, even if there are no changes in the documentation itself.

- [#1301](https://github.com/tigerbeetle/tigerbeetle/pull/1301)

  Fix outdated documentation regarding 128-bit balances.

- [#1302](https://github.com/tigerbeetle/tigerbeetle/pull/1302)

  Fix a [bug](https://github.com/tigerbeetle/tigerbeetle/issues/1290) discovered and reported during the [Hackathon 2023](https://github.com/tigerbeetle/hackathon-2023), where the Node.js client's error messages were truncated due to an incorrect string concatenation adding a null byte `0x00` in the middle of the string.

- [#1291](https://github.com/tigerbeetle/tigerbeetle/pull/1291)

  Update the Node.js samples instructions, guiding the user to install all dependencies before the sample project.

- [#1295](https://github.com/tigerbeetle/tigerbeetle/pull/1295)

  We've doubled the `Header`s size to 256 bytes, paving the way for future improvements that will require extra space. Concurrently, this change also refactors a great deal of code. Some of the `Header`'s fields are shared by all messages, however, each `Command` also requires specific pieces of information that are only used by its kind of message, and it was necessary to repurpose and reinterpret fields so that the same header could hold different data depending on the context. Now, commands have their own specialized data type containing the fields that are only pertinent to the context, making the API much safer and intent-clear.
  
- [#1304](https://github.com/tigerbeetle/tigerbeetle/pull/1304)

  With larger headers (see #1295) we have enough room to make the cluster ID a 128-bit integer, allowing operators to generate random cluster IDs without the cost of having a centralized ID coordinator. Also updates the documentation and sample programs to reflect the new maximum batch size, which was reduced from 8191 to 8190 items after we doubled the header.

### TigerTracks 🎧

- [She smiled sweetly](https://www.youtube.com/watch?v=fB1EpEFz6Lg)

## 2023-11-13

### Safety And Performance

- [#1264](https://github.com/tigerbeetle/tigerbeetle/pull/1264)

  Implement last-mile release artifact verification in CI.

- [#1268](https://github.com/tigerbeetle/tigerbeetle/pull/1268)

  Bump the simulator's safety phase max-ticks to avoid false positives from the liveness check.

- [#1270](https://github.com/tigerbeetle/tigerbeetle/pull/1270)

  Fix a crash caused by a race between a commit and a repair acquiring a client-reply `Write`.

- [#1278](https://github.com/tigerbeetle/tigerbeetle/pull/1278)

  Fix a crash caused by a race between state (table) sync and a move-table compaction.

  Both bugs didn't stand a chance in the [Line of Fire](https://www.youtube.com/watch?v=pq-G3EWO9XM)
  of our deterministic simulator!

### Internals

- [#1244](https://github.com/tigerbeetle/tigerbeetle/pull/1244)

  Specify which CPU features are supported in builds.

- [#1275](https://github.com/tigerbeetle/tigerbeetle/pull/1275)

  Improve `shell.zig`'s directory handling, to guard against mistakes with respect to the current
  working directory.

- [#1277](https://github.com/tigerbeetle/tigerbeetle/pull/1277)

  Interpret a git hash as a VOPR seed, to enable reproducible simulator smoke tests in CI.

- [#1288](https://github.com/tigerbeetle/tigerbeetle/pull/1288)

  Explicitly target glibc 2.7 when building client libraries, to make sure TigerBeetle clients are
  compatible with older distributions.

## 2023-11-06

### Safety And Performance

- [#1263](https://github.com/tigerbeetle/tigerbeetle/pull/1263)

  Revive the TigerBeetle [VOPRHub](https://github.com/tigerbeetle-vopr)! Some previous changes left
  it on it's [Last Stand](https://open.spotify.com/track/1ibHApXtb0pgplmNDRLHrJ), but the bot is
  back in business finding liveness bugs: [#1266](https://github.com/tigerbeetle/tigerbeetle/issues/1266)

### Features

- [#1260](https://github.com/tigerbeetle/tigerbeetle/pull/1260)

  Set the latest Docker image to track the latest release. Avoids language clients going out of sync
  with your default docker replica installations.

### Internals

- [#1261](https://github.com/tigerbeetle/tigerbeetle/pull/1261)

  Move website doc generation for https://docs.tigerbeetle.com/ into the main repo.

- [#1265](https://github.com/tigerbeetle/tigerbeetle/pull/1265),
  [#1243](https://github.com/tigerbeetle/tigerbeetle/pull/1243)

  Addressed some release quirks with the .NET and Go client builds.

## 2023-10-30

### Safety And Performance

- [#1251](https://github.com/tigerbeetle/tigerbeetle/pull/1251)

  Prove a tighter upper bound for the size of manifest log. With this new bound, manifest log is
  guaranteed to fit in allocated memory and is smaller. Additionally, manifest log compaction is
  paced depending on the current length of the log, balancing throughput and time-to-recovery.

- [#1198](https://github.com/tigerbeetle/tigerbeetle/pull/1198)

  Recommend using [ULID](https://github.com/ulid/spec) for event IDs. ULIDs are approximately sorted,
  which significantly improves common-case performance.

### Internals

- [#1218](https://github.com/tigerbeetle/tigerbeetle/pull/1218)

  Rewrite Node.js client implementation to use the common C client underneath. While clients for
  other languages already use the underlying C library, the Node.js client duplicated some code for
  historical reasons, but now we can leave that duplication in the past. [This Is A
  Photograph](https://www.youtube.com/watch?v=X0i7whWLW8M).

## 2023-10-25

### Safety And Performance

- [#1240](https://github.com/tigerbeetle/tigerbeetle/pull/1240)

  Increase block size to reduce latencies due to compaction work. Today, we use a simplistic
  schedule for compaction, which causes latency spikes at the end of the bar. While the future
  solution will implement a smarter compaction pacing to distribute the work more evenly, we can
  get a quick win by tweaking the block and the bar size, which naturally evens out latency spikes.

- [#1246](https://github.com/tigerbeetle/tigerbeetle/pull/1246)

  The new release process changed the names of the published artifacts (the version is no longer
  included in the name). This broke our quick start scripts, which we have fixed. Note that we are
  in the process of rolling out the new release process, so some unexpected breakage is expected.

- [#1239](https://github.com/tigerbeetle/tigerbeetle/pull/1239),
  [#1243](https://github.com/tigerbeetle/tigerbeetle/pull/1243)

  Speed up secondary index maintenance by statically distinguishing between insertions and
  updates. [Faster than the speed of night!](https://open.spotify.com/track/30oZqbcUROFLSru3WcN3bx)

### Features

- [#1245](https://github.com/tigerbeetle/tigerbeetle/pull/1245)

  Include Docker images in the release.

### Internals

- [#1234](https://github.com/tigerbeetle/tigerbeetle/pull/1234)

  Simplify superblock layout by using a linked list of blocks for manifest log, so that the
  superblock needs to store only two block references.

  P.S. Note the PR number!

## 2023-10-23

This is the start of the changelog. A lot happened before this point and is lost in the mist of git
history, but any notable change from this point on shall be captured by this document.

### Safety And Performance

- [#1225](https://github.com/tigerbeetle/tigerbeetle/pull/1225)

  Remove bloom filters. TigerBeetle implements more targeted optimizations for
  both positive and negative lookups, making bloom filters a net loss.

### Features

- [#1228](https://github.com/tigerbeetle/tigerbeetle/pull/1228)

  Increase alignment of data blocks to 128KiB (from 512 bytes). Larger alignment gives operators
  better control over physical layout of data on disk.

### Internals

- [#1201](https://github.com/tigerbeetle/tigerbeetle/pull/1201),
  [#1232](https://github.com/tigerbeetle/tigerbeetle/pull/1232)

  Overhaul of CI and release infrastructure. CI and releases are now driven by Zig code. The main
  branch is gated on integration tests for all clients.

  This is done in preparation for the first TigerBeetle release.

## Prehistory

For archeological inquiries, check out the state of the repository at the time of the first
changelog:

[https://github.com/tigerbeetle/tigerbeetle/](https://github.com/tigerbeetle/tigerbeetle/tree/d2d6484188ecc57680e8bde446b5d09b6f2d83ca)
