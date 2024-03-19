---
sidebar_position: 1
---

# Download

## Bootstrap Script

You can use the bootstrap script to download TigerBeetle:

### macOS/Linux

```shell
git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle && ./bootstrap.sh
```

### Windows

```powershell
git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle && .\bootstrap.ps1
```

### Build from Source

To build TigerBeetle from source, add `-build` as an argument to the bootstrap script.

## Direct Download

TigerBeetle is a single, small, statically linked binary, download it here:

|               | Linux                            | Windows                          | MacOS                             |
| :------------ | :------------------------------- | :------------------------------- | :-------------------------------- |
| x86_64        |[tigerbeetle-x86_64-linux.zip]    |[tigerbeetle-x86_64-windows.zip]  | [tigerbeetle-universal-macos.zip] |
| aarch64       |[tigerbeetle-aarch64-linux.zip]   |             N/A                  | [tigerbeetle-universal-macos.zip] |

[tigerbeetle-aarch64-linux.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-aarch64-linux.zip
[tigerbeetle-universal-macos.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-universal-macos.zip
[tigerbeetle-x86_64-linux.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-linux.zip
[tigerbeetle-x86_64-windows.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-windows.zip

## Docker

While not required (TigerBeetle is designed to be easy to deploy as a single binary), a Docker
container is also available:

<https://github.com/tigerbeetle/tigerbeetle/pkgs/container/tigerbeetle>

## Client Libraries

Client libraries for .Net, Go, Java, and NodeJS are available in the respective registries:

|               |                                                                           |
| :------------ | :------------------------------------------------------------------------ |
| .Net          |<https://www.nuget.org/packages/tigerbeetle>                               |
| Go            |<https://github.com/tigerbeetle/tigerbeetle-go>                            |
| Java          |<https://central.sonatype.com/artifact/com.tigerbeetle/tigerbeetle-java>   |
| NodeJS        |<https://www.npmjs.com/package/tigerbeetle-node>                           |

**Note:** Please make sure that the version of the client library used matches the version of
`tigerbeetle` binary.
