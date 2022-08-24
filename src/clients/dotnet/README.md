# TigerBeetle dotnet client

A C# client for [TigerBeetle](https://github.com/coilhq/tigerbeetle)

****

*TigerBeetle is a financial accounting database designed for mission-critical safety and performance to power the future of financial services.*

## Development Setup

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

Run the install script provided by TigerBeetle repo:

**Linux/MacOS**
```bash
cd tigerbeetle
scripts/install_zig.sh
```

**Windows**
```cmd
cd tigerbeetle
scripts\install_zig.bat
```

### 4. Test

On `tigerbeetle-dotnet/src` folder, run the integrated tests.

During the build, the `tb_client` library will be built and embedded into the dotnet project.

```bash
dotnet test
```

## Usage

TODO

## Other clients and documentation

- [Tigerbeetle Node](https://github.com/coilhq/tigerbeetle-node)
- [Tigerbeetle Go](https://github.com/coilhq/tigerbeetle-go)

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
