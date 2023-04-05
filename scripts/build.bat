:: Really simple script that mocks scripts/install.sh so that you can
:: simply `scripts/install.{os == .windows ? "bat" : "sh"}` in CI.

@echo off
setlocal enabledelayedexpansion

.\zig\zig.exe build %*
