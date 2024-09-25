#!/bin/bash

for SIZE in "10000" "100000" "1000000" "10000000" "100000000"; do
    python -m taps.run \
        --config configs/roundtrip.toml \
        --app.task-data-bytes $SIZE \
        --run.dir-format runs/roundtrip/baseline/{timestamp}/

    python -m taps.run \
        --config configs/roundtrip.toml configs/proxystore-daos.toml \
        --app.task-data-bytes $SIZE \
        --run.dir-format runs/roundtrip/proxystore-daos/{timestamp}/
done
