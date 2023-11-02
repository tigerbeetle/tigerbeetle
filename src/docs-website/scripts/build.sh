#!/usr/bin/env bash

set -eu

repo="https://github.com/tigerbeetledb/tigerbeetle"
root="$(pwd)"
rm -rf pages
cp -r ../../docs pages

# Rewrite links to clients
mkdir pages/clients
clients="go java dotnet node"
for client in $clients; do
    # READMEs are rewritten to a local path since they will be on the docs site.
    for page in $(find pages -type f); do
        # Need a relative path for the link checker to work.
        readme="$root/pages/clients/$client.md"
        relpath="$(realpath --relative-to="$(dirname $root/$page)" "$readme")"
        sed -i "s@/src/clients/$client/README.md@$relpath@g" "$page"
    done

    cp ../../src/clients/$client/README.md pages/clients/$client.md
done
echo '{ "label": "Client Libraries", "position": 6 }' >> pages/clients/_category_.json

# Everything else will be rewritten as a link into GitHub.
find pages -type f | xargs -I {} sed -i "s@/src/clients/@$repo/blob/main/src/clients/@g" {}

for page in $(ls pages/*.md); do
    if ! [[ "$page" == "pages/intro.md" ]] && ! [[ "$page" == "pages/FAQ.md" ]]; then
        rm "$page"
    fi
done

# Validate links
npx remark --use remark-validate-links --frail pages

# Build the site
rm -rf docs build
npx docusaurus build
cp -r build docs

# CNAME file for Github Pages DNS matching
echo 'docs.tigerbeetle.com' >> docs/CNAME
