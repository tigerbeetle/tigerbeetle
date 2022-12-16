# tigerbeetle-java

[TigerBeetle](https://github.com/tigerbeetledb/tigerbeetle) client for Java.


## Pre-built package

Available at [GitHub Packages Registry](https://github.com/orgs/tigerbeetledb/packages?repo_name=tigerbeetle) as a `jar` package.

You can install it by just downloading and placing the `jar` package directly in your `classpath` or by using a package management system such as [Maven](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry) or [Gradle](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry).

## Examples

See the [examples page](./examples/README.md) for more details.

## API Usage

A client needs to be configured with a `clusterID` and `replicaAddresses`. 
The `Client` class is thread-safe and for better performance, a single instance should be shared between multiple concurrent tasks.
Multiple clients can be instantiated in case of connecting to more than one TigerBeetle cluster.

> Your operating system should be Linux (kernel >= v5.6), macOS or Windows.

```java
import com.tigerbeetle.Client;

// ...

Client client = new Client(0, new String[] { "127.0.0.1:3001", "127.0.0.1:3002", "127.0.0.1:3003" });
```

### Account Creation

All TigerBeetle's IDs are 128-bit unsigned integers that can be represented as a `byte[]` array of 16 bytes or a pair of `long`.

```java
import com.tigerbeetle.Client;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.CreateAccountResultBatch;

// ...

// Creates a batch of accounts with the desired capacity
AccountBatch accounts = new AccountBatch(100);

// loop adding N elements into the batch
accounts.add();
accounts.setId(id);
accounts.setCode(100);
accounts.setLedger(720);

// Blocking usage:
// Submit the batch and waits for reply
CreateAccountResultBatch errors = client.createAccounts(accounts);

// Results are returned only for the accounts that failed.
while(errors.next()) {

    switch (accountErrors.getResult()) {

        case Exists:
            System.err.printf("Account at %d already exists.\n",
                errors.getIndex());        
            break;

        default:

            System.err.printf("Error creating account at %d: %s\n",
                errors.getIndex(),
                errors.getResult());
            break;
    }
}
```

Successfully executed events return an empty array whilst unsuccessful ones return an array with errors for only the ones that failed. An error will point to the index in the submitted array of the failed event.

### Creating a Transfer

Amounts are 64-bit unsigned integers values.

```java
import com.tigerbeetle.Client;
import com.tigerbeetle.TransferBatch;
import com.tigerbeetle.CreateTransferResultBatch;
import java.util.concurrent.CompletableFuture;

// ...

// Creates a batch of transfers with the desired capacity
TransferBatch transfers = new TransferBatch(100);

// loop adding N elements into the batch
transfers.add();
transfers.setId(id);
transfers.setCreditAccountId(creditAccountId);
transfers.setDebitAccountId(debitAccountId);
transfers.setCode(3001);
transfers.setLedger(720);
transfers.setAmount(100);

// Async usage:
// Submit the batch and returns immediately
CompletableFuture<CreateTransferResultBatch> request = client.createTransfersAsync(transfers);

// Register something on the application's side while TigerBeetle is processing
// UPDATE MyCustomer ...

// Gets the reply
CreateTransferResultBatch errors = request.get();

// Results are returned only for the transfers that failed.
while(errors.next()) {

    switch (accountErrors.getResult()) {

        case ExceedsCredits:
            System.err.printf("Transfer at %d exceeds credits.\n",
                errors.getIndex());        
            break;

        default:

            System.err.printf("Error creating transfer at %d: %s\n",
                errors.getIndex(),
                errors.getResult());
            break;
    }
}    
```

## Building from source 

*Prerequisites:*

- JDK 11+
- [Maven](https://maven.apache.org) 3.1+

### 1. Clone this repository

```bash
git clone https://github.com/tigerbeetledb/tigerbeetle.git
cd tigerbeetle
```
### 2. Install Zig and Tigerbeetle

**Linux and macOS**

```bash
cd src/clients/java
./scripts/install.sh
```

**Windows**

```cmd
cd src\clients\java
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

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
