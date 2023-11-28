---
sidebar_position: 2
---

# Installation

TigerBeetle is a single, small, statically linked binary, download it here:

|               | Linux                            | Windows                          | MacOS                             |
| :------------ | :------------------------------- | :------------------------------- | :-------------------------------- |
| x86_64        |[tigerbeetle-x86_64-linux.zip]    |[tigerbeetle-x86_64-windows.zip]  | [tigerbeetle-universal-macos.zip] |
| aarch64       |[tigerbeetle-aarch64-linux.zip]   |             N/A                  | [tigerbeetle-universal-macos.zip] |

[tigerbeetle-aarch64-linux.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-aarch64-linux.zip
[tigerbeetle-universal-macos.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-universal-macos.zip
[tigerbeetle-x86_64-linux.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-linux.zip
[tigerbeetle-x86_64-windows.zip]: https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-windows.zip

While not required (TigerBeetle is designed to be easy to deploy as a single binary), a Docker
container is also available:

<https://github.com/tigerbeetle/tigerbeetle/pkgs/container/tigerbeetle>


Client libraries for .Net, Go, Java, and NodeJS are available in the respective registries:

|               |                                                                           |
| :------------ | :------------------------------------------------------------------------ |
| .Net          |<https://www.nuget.org/packages/tigerbeetle>                               |
| Go            |<https://github.com/tigerbeetle/tigerbeetle-go>                            |
| Java          |<https://central.sonatype.com/artifact/com.tigerbeetle/tigerbeetle-java>   |
| NodeJS        |<https://www.npmjs.com/package/tigerbeetle-node>                           |

**Note:** TigerBeetle is not yet production-ready. In particular, the protocol and data file formats
may change and might not be compatible across different releases. Please make sure that the version
of the client library used matches the version of `tigerbeetle` binary.
