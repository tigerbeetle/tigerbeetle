#!/usr/bin/env bash

#
# NOTE: Don't call this script directly. It produces different results
# for the cached/hash file names on different systems.
#

# Build a different branch by passing the branch name in as the first argument
# Build a different repo by passing the repo URL as the second argument (for forks)
branch="main"
if [[ -n "$1" ]]; then
    branch="$1"
fi
repo="https://github.com/tigerbeetledb/tigerbeetle"
if [[ -n "$2" ]]; then
    repo="$2"
fi

echo "Building branch: $branch from $repo"

set -eux

# Grab the latest docs from tigerbeetledb/tigerbeetle
rm -rf tb_tmp
git clone "$repo" tb_tmp

if ! [[ "$branch" == "main" ]]; then
    ( cd tb_tmp && git fetch && git checkout "$branch" )
fi

root="$(pwd)"
rm -rf pages
mv tb_tmp/docs pages

# Rewrite links to clients
if [[ -f tb_tmp/src/clients/integration.zig ]]; then # Skip until the updated clients docs PR is merged
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

	cp tb_tmp/src/clients/$client/README.md pages/clients/$client.md
    done
    echo '{ "label": "Client Libraries", "position": 6 }' >> pages/clients/_category_.json
    # Everything else will be rewritten as a link into GitHub.
    find pages -type f | xargs -I {} sed -i "s@/src/clients/@$repo/blob/$branch/src/clients/@g" {}
fi

for page in $(ls pages/*.md); do
    if ! [[ "$page" == "pages/intro.md" ]] && ! [[ "$page" == "pages/FAQ.md" ]]; then
	rm "$page"
    fi
done

rm -rf tb_tmp

# Validate links
npx remark --use remark-validate-links --frail pages

# Build the site
rm -rf docs build
npx docusaurus build
cp -r build docs

# CNAME file for Github Pages DNS matching
echo 'docs.tigerbeetle.com' >> docs/CNAME
