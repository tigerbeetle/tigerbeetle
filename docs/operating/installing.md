# Installing

## Quick Install

<details open>
<summary>Linux</summary>

```console
curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip
./tigerbeetle version
```
</details>

<details>
<summary>macOS</summary>

```console
curl -Lo tigerbeetle.zip https://mac.tigerbeetle.com && unzip tigerbeetle.zip
./tigerbeetle version
```
</details>

<details>
<summary>Windows</summary>

```console
powershell -command "curl.exe -Lo tigerbeetle.zip https://windows.tigerbeetle.com; Expand-Archive tigerbeetle.zip ."
.\tigerbeetle version
```
</details>

## Latest Release

You can download prebuilt binaries for the latest release here:

|         | Linux                           | Windows                          | MacOS                             |
| :------ | :------------------------------ | :------------------------------- | :-------------------------------- |
| x86_64  | [tigerbeetle-x86_64-linux.zip]  | [tigerbeetle-x86_64-windows.zip] | [tigerbeetle-universal-macos.zip] |
| aarch64 | [tigerbeetle-aarch64-linux.zip] | N/A                              | [tigerbeetle-universal-macos.zip] |

[tigerbeetle-aarch64-linux.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-aarch64-linux.zip
[tigerbeetle-universal-macos.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-universal-macos.zip
[tigerbeetle-x86_64-linux.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-linux.zip
[tigerbeetle-x86_64-windows.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-windows.zip

## Past Releases

The releases page lists all past and current releases:

<https://github.com/tigerbeetle/tigerbeetle/releases>

TigerBeetle can be upgraded without downtime, this is documented in [Upgrading](./upgrading.md).

## Building from Source

Building from source is easy, but is not recommended for production deployments, as extra care is
needed to ensure compatibility with clients and upgradability. Refer to the
[internal documentation](https://github.com/tigerbeetle/tigerbeetle/tree/main/docs/internals) for
compilation instructions.

## Client Libraries

Client libraries for .NET, Go, Java, Node.js, and Python are published to the respective package
repositories, see [Clients](../coding/clients/).
