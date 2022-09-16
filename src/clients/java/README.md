# tigerbeetle-java

[TigerBeetle](https://github.com/tigerbeetledb/tigerbeetle) client for Java.


## Pre-built package

Available at [GitHub Packages Registry](https://github.com/orgs/tigerbeetledb/packages?repo_name=tigerbeetle-java) as a `jar` package.

You can install it by just downloading and placing the `jar` package directly in your `classpath` or by using a package management system such as [Maven](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry) or [Gradle](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry).

## Usage

A client needs to be configured with a `clusterID` and `replicaAddresses`. 
The `Client` class is thread-safe and for better performance, a single instance should be shared between multiple concurrent tasks.
Multiple clients can be instantiated in case of connecting to more than one TigerBeetle cluster.

> Your operating system should be Linux (kernel >= v5.6) or macOS. Windows support is not yet available.

```java
import com.tigerbeetle.Client;

// ...

Client client = new Client(0, new String[] { "127.0.0.1:3001", "127.0.0.1:3002", "127.0.0.1:3003" });
```

### Account Creation

All TigerBeetle's ID are 128-bit integer, and `tigerbeetle-java` uses a `java.util.UUID` to represent this value.

```java
import com.tigerbeetle.Client;
import com.tigerbeetle.Account;
import com.tigerbeetle.AccountsBatch;
import com.tigerbeetle.CreateAccountsResult;

// ...

// Creates a batch with the desired capacity
AccountsBatch batch = new AccountsBatch(100);

Account account1 = new Account();
account1.setId(UUID.randomUUID());
account1.setCode(100);
account1.setLedger(720);

// Add N elements into the batch
batch.add(account1);

// Blocking usage:
// Submit the batch and waits for reply
CreateAccountsResult[] errors = client.createAccounts(batch);
if (errors.length > 0)
    throw new Exception("Unexpected createAccount results");
```

Successfully executed events return an empty array whilst unsuccessful ones return an array with errors for only the ones that failed. An error will point to the index in the submitted array of the failed event.

### Creating a Transfer

Amounts are 64-bit unsigned integers values.

```java
import com.tigerbeetle.Client;
import com.tigerbeetle.Transfer;
import com.tigerbeetle.TransfersBatch;
import com.tigerbeetle.CreateTransfersResult;

// ...

// Creates a batch with the desired capacity
TransfersBatch batch = new TransfersBatch(100);

Transfer transfer1 = new Transfer();
transfer1.setId(UUID.randomUUID());
transfer1.setCreditAccountId(account1.getId());
transfer1.setDebitAccountId(account2.getId());
transfer1.setCode(1);
transfer1.setLedger(720);
transfer1.setAmount(100);

// Add N elements into the batch
batch.add(transfer1);

// Async usage:
// Submit the batch and returns immediately
Future<CreateTransfersResult[]> request = client.createTransfersAsync(batch);

// Register something on the application's side while tigerbeetle is processing
// UPDATE MyCustomer ...

// Gets the reply
CreateTransfersResult[] errors = request.get();
if (errors.length > 0)
    throw new Exception("Unexpected transfer results");

```

## Building from source 

*Prerequisites:*

- JDK 11+
- [Maven](https://maven.apache.org) 3.1+

### 1. Clone this repository

Clone loading the submodules

```bash
git clone --recurse-submodules https://github.com/tigerbeetledb/tigerbeetle-java.git
```

Or initialize the submodules after cloning

```bash
git submodule init
git submodule update 
```
### 2. Install Zig and Tigerbeetle

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

### 3. Run the benchmark

**Linux and macOS**

```bash
./scripts/benchmark.sh
```

**Windows**

```cmd
.\scripts\benchmark.bat
```

## Other clients and documentation

- [Tigerbeetle Node](https://github.com/tigerbeetledb/tigerbeetle-node)
- [Tigerbeetle Go](https://github.com/tigerbeetledb/tigerbeetle-go)

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
