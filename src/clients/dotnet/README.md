# TigerBeetle dotnet client

A C# client for [TigerBeetle](https://github.com/coilhq/tigerbeetle)

****

*TigerBeetle is a financial accounting database designed for mission-critical safety and performance to power the future of financial services.*

## Development Setup

[![asciicast](https://asciinema.org/a/518513.svg)](https://asciinema.org/a/518513)

### 1. Install [dotnet 6](https://dotnet.microsoft.com/en-us/download)

It is needed to build and run the tests, this library can be used from any [.Net Standard 2.1 compatible runtime](https://docs.microsoft.com/en-us/dotnet/standard/net-standard) (.Net Framework 4.8, .Net Core, Xamarin, etc)


For Windows users, there is a `.sln` solution file that can be opened with `Microsoft Visual Studio 2022`.

### 2. Clone this repo

Clone loading the submodules

```bash
git clone --recurse-submodules https://github.com/batiati/tigerbeetle-dotnet.git
```

Or initialize the submodules after cloning

```bash
git submodule init
git submodule update 
```

### 3. Install TigerBeetle

Run the install script provided by TigerBeetle repo, and build the server executable.

**Linux/MacOS**
```bash
cd tigerbeetle
scripts/install.sh
```

**Windows**
```cmd
cd tigerbeetle
scripts\install_zig.bat
.\zig\zig.exe build install -Drelease-safe
mv .\zig-out\bin\tigerbeetle.exe .
```

### 4. Test

On `tigerbeetle-dotnet/src` folder, run the integrated tests.

During the build, the `tb_client` library will be built and embedded into the dotnet project.

```bash
dotnet test
```

## Usage

A client needs to be configured with a `clusterID` and `replicaAddresses`.
The `Client` class is thread-safe and for better performance, a single instance should be shared between multiple concurrent tasks.
Multiple clients can be instantiated in case of connecting to more than one TigerBeetle cluster.

```C#
var client = new Client(clusterID: 0, replicaAddresses: new[] { "3001", "3002", "3003" });
```

### Account Creation

All TigerBeetle's ID are 128-bit integer, and `tigerbeetle-dotnet` client accepts a wide range of dotnet values: `int`, `uint`, `long`, `ulong`, `Guid` and the custom type `UInt128`.

```C#
var accounts = new[] {
    new Account
    {
        Id = 1,
        UserData = Guid.NewGuid(),
        Code = 2,
        Ledger = ISO4217.USD.Code,
    },  
};

var errors = await client.CreateAccounts(accounts);
```

Successfully executed events return an empty array whilst unsuccessful ones return an array with errors for only the ones that failed. An error will point to the index in the submitted array of the failed event.

### Creating a Transfer

Amounts are 64-bit unsigned integers and can be easily converted to/from `decimal` values.

```C#
transfers = new[] {
    new Transfer
    {
        Id = Guid.NewGuid(),
        DebitAccountId = accountA.Id,
        CreditAccountId = accountB.Id,
        Code = 1,
        Ledger = ISO4217.USD.Code,
        Amount = ISO4217.USD.ToUInt64(0.99M),
    },
}

var errors = await client.CreateTransfers(accounts);
```

## Other clients and documentation

- [Tigerbeetle Node](https://github.com/coilhq/tigerbeetle-node)
- [Tigerbeetle Go](https://github.com/coilhq/tigerbeetle-go)

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
