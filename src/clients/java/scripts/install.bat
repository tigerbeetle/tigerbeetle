@echo off
setlocal enabledelayedexpansion

git submodule init
git submodule update

cd ..\..\.. 
call .\scripts\install.bat

echo "Building TigerBeetle Java Client..."
mvn -B package --quiet
