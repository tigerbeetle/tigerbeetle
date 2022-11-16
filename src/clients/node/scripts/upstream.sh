#!/bin/bash
set -e

echo "Updating upstream to latest commit on main..."
cd src/tigerbeetle
git checkout main
git pull
cd ../..
git add src/tigerbeetle
git status
echo "Ready to commit src/tigerbeetle"
echo ""
