@echo off
setlocal enabledelayedexpansion

git submodule init
git submodule update

pushd ..\..\..
call .\scripts\install.bat
popd

echo "Building TigerBeetle Java Client..."
mvn -e -B package -Dmaven.test.skip -Djacoco.skip --quiet
