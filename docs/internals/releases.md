---
sidebar_position: 6
---

# Releases

How a new TigerBeetle release is made. Note that the process is being established, so this document
might not perfectly reflect reality just yet.

## What Is a Release?

TigerBeetle is distributed in binary form. There are two main reasons for this:

- to ensure correctness and rule out large classes of configuration errors, the actual machine code
  must be tested.
- Zig is not stable yet. Binary releases insulate the users from this instability and keep the
  language as an implementation detail.

TigerBeetle binary is released in lockstep with client libraries. At the moment, implementation
of the client libraries is tightly integrated and shares code with TigerBeetle, requiring matching
versions.

Canonical form of the "release" is a `dist/` folder with the following artifacts:

- `tigerbeetle/` subdirectory with `.zip` archived `tigerbeetle` binaries built for all supported
  architectures.
- `dotnet/` subdirectory with a Nuget package.
- `go/` subdirectory with the source code of the go client and precompiled native libraries for
  all supported platforms.
- `java/` subdirectory with a `.jar` file.
- `node/` subdirectory with a `.tgz` package for npm.

## Publishing

Release artifacts are uploaded to appropriate package registries. GitHub release is used for
synchronization:

- a draft release is created at the start of the publishing process,
- artifacts are uploaded to GitHub releases, npm, Maven Central, and Nuget. For Go, a new commit
  is pushed to <https://github.com/tigerbeetle/tigerbeetle-go>
- if publishing to all registries were successfully, the release is marked as non-draft.

All publishing keys are stored as GitHub Actions secrets.

## Triggering a Release

Releases are triggered manually, on Monday, by pushing the the current tip of `origin/main` to
`origin/release`. This triggers the `release.yml` workflow. The workflow doesn't run unit-tests,
[not rocket science](https://graydon2.dreamwidth.org/1597.html) rule is assumed.

## Versioning

Because releases are frequent, we avoid specifying the version in the source code. Instead, the
version is setup automatically by the release process itself, using sequential build numbers as the
patch version. It might a case that a particular version is skipped (e.g., if a release partially
failed). It is also a possibility that two releases are functionally identical (this means that no
pull requests were merged in a week).
