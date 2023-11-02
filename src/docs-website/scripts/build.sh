# Important not to use node:19-slim since slim doesn't include `git`
# and this does weird things when using that as the container in
# Github Actions.

# Furthermore, for some reason in Github Actions (only), it complains
# about "fatal: detected dubious ownership in repository" without this
# add safe directory command.
docker run -v $(pwd):/build -w /build node:19 bash -c "git config --global --add safe.directory /build && npm install && ./scripts/build_dont_call_this_directly.sh $*"
