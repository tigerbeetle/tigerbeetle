# TigerBeetle java client

A Java client for [TigerBeetle](https://github.com/coilhq/tigerbeetle)

****

*TigerBeetle is a financial accounting database designed for mission-critical safety and performance to power the future of financial services.*

## Development Setup

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

### 4. Run the benchmark

**Linux and macOS**

```bash
./scripts/benchmark.sh
```

## Other clients and documentation

- [Tigerbeetle Node](https://github.com/coilhq/tigerbeetle-node)
- [Tigerbeetle Go](https://github.com/coilhq/tigerbeetle-go)
- [Tigerbeetle C#](https://github.com/batiati/tigerbeetle-dotnet)

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
