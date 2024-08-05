#!/bin/bash

redis-server --port 6379 --save "" --appendonly no --protected-mode no &> /dev/null &
REDIS=$!
echo "Redis server started"

for i in {1..3}; do
    python -m taps.run \
        --run.dir_format runs/{name}/baseline/{timestamp} \
        --config configs/cholesky-app.toml configs/dask-local.toml
done

for i in {1..3}; do
    python -m taps.run \
        --run.dir_format runs/{name}/proxystore/{timestamp} \
        --config configs/cholesky-app.toml configs/dask-local.toml \
        configs/proxystore-redis-local.toml
done

kill $REDIS
echo "Redis server stopped"
