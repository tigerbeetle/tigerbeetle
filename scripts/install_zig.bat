@echo off

set ZIG_RELEASE_DEFAULT=0.11.0

:: Determine the Zig build:
if "%~1"=="" (
    set ZIG_RELEASE=%ZIG_RELEASE_DEFAULT%
) else if "%~1"=="latest" (
    set ZIG_RELEASE=builds
) else (
    set ZIG_RELEASE=%~1
)

:: Checks format of release version.
echo.%ZIG_RELEASE% | findstr /b /r /c:"builds" /c:"^[0-9][0-9]*.[0-9][0-9]*.[0-9][0-9]*">nul || (echo.Unexpected release format. && exit 1)

set ZIG_OS=windows
set ZIG_ARCH=x86_64

set ZIG_TARGET=zig-%ZIG_OS%-%ZIG_ARCH%

:: Determine the build, split the JSON line on whitespace and extract the 2nd field:
for /f "tokens=2" %%a in ('curl --silent https://ziglang.org/download/index.json ^| findstr %ZIG_TARGET% ^| findstr %ZIG_RELEASE%' ) do (
  set ZIG_URL=%%a
)

:: Then remove quotes and commas:
for /f %%b in ("%ZIG_URL:,=%") do (
    set ZIG_URL=%%~b
)

:: Checks the ZIG_URL variable follows the expected format.
echo.%ZIG_URL% | findstr /b /r /c:"https://ziglang.org/builds/" /c:"https://ziglang.org/download/%ZIG_RELEASE%">nul || (echo.Unexpected release URL format. && exit 1)

if "%ZIG_RELEASE%"=="builds" (
    echo Installing Zig latest build...
) else (
    echo Installing Zig %ZIG_RELEASE% release build...
)

:: Using variable modifiers to determine the directory and filename from the URL:
:: %%~ni Expands %%i to a file name only and %%~xi Expands %%i to a file name extension only.
for /f %%i in ("%ZIG_URL%") do (
    set ZIG_DIRECTORY=%%~ni
    set ZIG_TARBALL=%%~nxi
)

:: Checks the ZIG_DIRECTORY variable follows the expected format.
echo.%ZIG_DIRECTORY% | findstr /b /r /c:"zig-win64-" /c:"zig-windows-x86_64-">nul || (echo.Unexpected zip directory name format. && exit 1)

:: Making sure we download to the same output document, without wget adding "-1" etc. if the file was previously partially downloaded:
if exist %ZIG_TARBALL% (
  del /q %ZIG_TARBALL%
  if exist %ZIG_TARBALL% (
    echo Failed to delete %ZIG_TARBALL%.
    exit 1
  )
)

echo Downloading %ZIG_URL%...
curl --silent --progress-bar --output %ZIG_TARBALL% %ZIG_URL%
if not exist  %ZIG_TARBALL% (
  echo Failed to download Zig zip file.
  exit 1
)

:: Replace any existing Zig installation so that we can install or upgrade:
echo Removing any existing 'zig' and %ZIG_DIRECTORY% folders before extracting.
if exist zig\ (
  rd /s /q zig\
  :: Ensure the directory has been deleted.
  if exist zig\ (
    echo The ‘zig’ directory could not be deleted.
    exit 1
  )
)

if exist %ZIG_DIRECTORY%\ (
  rd /s /q %ZIG_DIRECTORY%
  :: Ensure the directory has been deleted.
  if exist %ZIG_DIRECTORY% (
    echo The %ZIG_DIRECTORY% directory could not be deleted.
    exit 1
  )
)

:: Extract and then remove the downloaded tarball:
echo Extracting %ZIG_TARBALL%...

:: Hiding Powershell's progress bar during the extraction
SET PS_DISABLE_PROGRESS="$ProgressPreference=[System.Management.Automation.ActionPreference]::SilentlyContinue"
powershell -Command "%PS_DISABLE_PROGRESS%;Expand-Archive %ZIG_TARBALL% -DestinationPath ."

if not exist %ZIG_TARBALL% (
  echo Failed to extract zip file.
  exit 1
)

echo Installing %ZIG_DIRECTORY% to 'zig' in current working directory...
ren %ZIG_DIRECTORY% zig
if exist %ZIG_DIRECTORY% (
  echo Failed to rename %ZIG_DIRECTORY% to zig.
  exit 1
)

:: Removes the zip file
del /q %ZIG_TARBALL%
if exist %ZIG_TARBALL% (
  echo Failed to delete %ZIG_TARBALL% file.
  exit 1
)

echo "Congratulations, you have successfully installed Zig version %ZIG_RELEASE%. Enjoy!"
