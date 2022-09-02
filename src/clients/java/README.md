# TigerBeetle java client

A Java client for [TigerBeetle](https://github.com/coilhq/tigerbeetle)

****

*TigerBeetle is a financial accounting database designed for mission-critical safety and performance to power the future of financial services.*

## Development Setup

[![asciicast](https://asciinema.org/a/518508.svg)](https://asciinema.org/a/518508)

### 1. Install Java SDK for your platform

Be sure to install the JDK not the JRE, example `java-17-openjdk-devel`.

You can check by typing
```bash
javac -version
```

### 2. Clone this repository

Clone loading the submodules

```bash
git clone --recurse-submodules https://github.com/batiati/tigerbeetle-java.git
```

Or initialize the submodules after cloning

```bash
git submodule init
git submodule update 
```
### 3. Install Zig and Tigerbeetle

**Linux and macOS**

```bash
cd tigerbeetle-java
./scripts/install.sh
```

**Windows**

```cmd
cd tigerbeetle-java
.\scripts\install.bat
```

### 4. Run the benchmark

**Linux and macOS**

```bash
./scripts/benchmark.sh
```

**Windows**

```cmd
.\scripts\benchmark.bat
```

## Usage

A client needs to be configured with a `clusterID` and `replicaAddresses`.
The `Client` class is thread-safe and for better performance, a single instance should be shared between multiple concurrent tasks.
Multiple clients can be instantiated in case of connecting to more than one TigerBeetle cluster.

```java
var client = new Client(0, new String[] { "3001", "3002", "3003" });
```

### Account Creation

All TigerBeetle's ID are 128-bit integer, and `tigerbeetle-java` uses a `java.util.UUID` to represent this value.

```java

// Creates a batch with the desired capacity
var batch = new AccountsBatch(100);

var account1 = new Account();
account1.setId(UUID.randomUUID());
account1.setCode(100);
account1.setLedger(720);

// Add N elements into the batch
batch.add(account1);

// Blocking usage:
// Submit the batch and waits for reply
var errors = client.createAccounts(batch);
if (results.length > 0)
    throw new Exception("Unexpected createAccount results");
```

Successfully executed events return an empty array whilst unsuccessful ones return an array with errors for only the ones that failed. An error will point to the index in the submitted array of the failed event.

### Creating a Transfer

Amounts are 64-bit unsigned integers values.

```java

// Creates a batch wit// Creates a batch with the desired capacity
var transfer1 = new Transfer();
transfer1.setId(UUID.randomUUID());
transfer1.setCreditAccountId(account1.getId());
transfer1.setDebitAccountId(account2.getId());
transfer1.setCode((short) 1);
transfer1.setLedger(720);
transfer1.setAmount(100);

// Add N elements into the batch
batch.add(transfer1);

// Async usage:
// Submit the batch and returns immediately
var request = client.createTransfersAsync(batch);

// Register something on the application's side while tigerbeetle is processing
// UPDATE MyCustomer ...

// Gets the result
var errors = request.get();
if (errors.length > 0)
    throw new Exception("Unexpected transfer results");

```

## Other clients and documentation

- [Tigerbeetle Node](https://github.com/coilhq/tigerbeetle-node)
- [Tigerbeetle Go](https://github.com/coilhq/tigerbeetle-go)
- [Tigerbeetle C#](https://github.com/batiati/tigerbeetle-dotnet)

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
