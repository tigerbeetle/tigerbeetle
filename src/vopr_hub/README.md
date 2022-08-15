# The VOPR Hub

*The VOPR Hub collects the seeds for crash, liveness and correctness reports from the VOPR simulators running permanently in CI or on local dev machines. The VOPR Hub then checks out that commit, replays the seed locally in Debug mode, collects the logs, and automatically creates a GitHub issue, also deduplicating against existing issues if necessary.*

## The VOPR

*The Viewstamped Operation Replicator* provides deterministic simulation testing for TigerBeetle. It tests that clusters of TigerBeetle replicas and clients interact correctly according to TigerBeetle's Viewstamped Replication consensus protocol, even under the pressure of simulated network and storage faults.

The VOPR has an optional `--send` flag that enables it to send bug reports to the VOPR Hub. This flag can only be used when any local code changes have been committed and pushed. The reason for this is that for the hub to replay a failing seed it needs to run that seed on the same commit to get the same result.

If the VOPR discovers a failing seed it creates a bug report in the format of a fixed length byte array.

* 16 bytes contain the first half of a SHA256 hash of the remainder of the message, which is used both as a checksum, and to reject clearly misdirected or random network packets.
* 1 byte indicates the type of bug detected (correctness, liveness, or crash).
* 8 bytes are reserved for the seed.
* The final 20 bytes contain the hash of the git commit to which the seed applies.

## Hub Logic

The hub listens for bug reports sent by any VOPR via TCP.

### Setup
The VOPR Hub must run a VOPR in a separate tigerbeetle directory to prevent it from checking out a commit that could change the hub itself.

The VOPR which the hub runs must be run inside its own tigerbeetle directory in order to parse its output correctly.

To run the VOPR Hub, Zig must be installed and five environmental variables are required:
1. TIGERBEETLE_DIRECTORY the location of the VOPR's tigerbeetle directory
2. ISSUE_DIRECTORY where issues are stored on disk
3. DEVELOPER_TOKEN for access to GitHub
4. VOPR_HUB_ADDRESS for the IP address to listen on for incoming messages
5. REPOSITORY_URL to post the GitHub issue

### Validation

When the hub receives a message it first validates it immediately. Messages are expected to be exactly 45 bytes in length. The hub hashes the last 29 bytes of the message and ensures that the first half of the SHA256 hash matches the first 16 bytes of the message. If the hash is correct then the hub checks that the first byte (representing the bug type) is between 1 and 3. After the 64-bit unsigned seed, the remaining 20 bytes are the GitHub commit hash, which must all decode to valid hex characters.

Once validated, the message is decoded and added to a queue for processing.

### Replies to the VOPR

Once the message is determined to be valid then a reply of "1" is sent back to the VOPR and the connection is closed. If it's invalid, the connection is simply closed (following the principle of “Don't talk to strangers”) and no further processing is applied.

### Message Processing

When the hub replays a seed it will save the logs to disk. This way each issue can be tracked to see if it has already been submitted. For correctness bugs (bug 1) and liveness bugs (bug 2) the format of the file name is `bug_seed_commit`. Correctness and liveness bugs can be deduped immediately by checking for their file name on disk. Crash bugs (bug 3) do not include the seed in their file name but do have an additional field which is the hash of the stack trace of the issue (`bug_commit_stacktracehash`) since multiple seeds may all trip the same assertion, leading to the same stack trace. Therefore, crash bugs can only be deduped after the seed has been replayed and the logs have been generated.

If no duplicate issue has been found then the hub will replay the seed in `Debug` mode and capture the logs. In order to do this it must first checkout the correct git commit. This step requires that the reported commit is available in the `tigerbeetle` repository.

### Create an Issue

Once the simulation has completed the stack trace is extracted and parsed to remove the local directory structure and any local memory addresses so that it is not only deterministic across machines, but also anonymized. This way it can be hashed and used to dedupe any crash bugs that may have already been reported. While crash bugs include a hash of the stack trace in their filename to deduplicate assertion crashes for the same call graph, we do not do this for correctness bugs, since these are always detected by the same set of panics in the simulator, at the same call site, but there may be different causes for them.

A copy of the issue is written to disk and a GitHub issue is also automatically generated. The issue contains the bug type, seed, commit hash, parameters of the VOPR, stack trace (if there is one), and debug logs.

If the VOPR Hub replays a seed and it passes unexpectedly then, to err on the safe side, a GitHub issue will still be created with a warning explaining that the seed passed.

The VOPR Hub may only have up to GitHub issues open at any time. These issues are identified by the `vopr` label that is attached to them as they are created. If there are 6 open issues and the VOPR Hub finds an additional issue then it is simply logged locally on the machine.
