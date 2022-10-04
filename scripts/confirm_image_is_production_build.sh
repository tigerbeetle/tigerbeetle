set -e

if [[ $(docker run --entrypoint nm $1 -an /opt/beta-beetle/tigerbeetle | grep getSymbolFromDward) ]]; then
    echo 'Does not seem to be a production build'
    exit 1
fi
