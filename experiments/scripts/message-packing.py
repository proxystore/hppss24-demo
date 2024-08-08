from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Sequence

import msgpack
import numpy
from distributed.protocol import deserialize, serialize
from distributed.protocol.utils import msgpack_opts
from proxystore.connectors.local import LocalConnector
from proxystore.store import Store
from proxystore.store.utils import get_key


class PatchedLocalConnector(LocalConnector):
    def config(self) -> dict[str, Any]:
        return {}


def benchmark(size: int, store: Store[Any] | None) -> (float, float):
    data = numpy.random.rand(size // 8)

    ser_start = time.perf_counter()
    if store is not None:
        data = store.proxy(data)
    header, frames = serialize(data)
    message = msgpack.dumps((header, frames), use_bin_type=True)
    ser_end = time.perf_counter()

    de_start = time.perf_counter()
    header, frames = msgpack.loads(message, use_list=False, **msgpack_opts)
    data = deserialize(header, frames)
    assert data[0] >= 0
    de_end = time.perf_counter()

    if store is not None:
        store.evict(get_key(data))

    return ser_end - ser_start, de_end - de_start


def run(
    *,
    output: str,
    repeat: int,
    sizes: list[int],
    tag: str,
    use_proxystore: bool,
) -> None:
    store: Store[Any] | None = None
    if use_proxystore:
        store = Store(
            "bench",
            PatchedLocalConnector(),
            cache_size=0,
            register=True,
        )

    results = []

    for size in sizes:
        print(f"Benchmarking with {size} bytes (repeat={repeat})")
        for _ in range(repeat):
            times = benchmark(size=size, store=store)
            result = {
                "Tag": tag,
                "ProxyStore": use_proxystore,
                "Message Size": size,
                "Serialize": times[0],
                "Deserialize": times[1],
            }
            results.append(result)

    if store is not None:
        store.close()

    with open(output, "a") as f:
        for result in results:
            f.write(json.dumps(result))
            f.write('\n')

    print(f"Wrote {len(results)} to {output}")


def main(argv: Sequence[str] | None = None) -> int:
    argv = sys.argv if argv is None else argv

    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", help="tag for run")
    parser.add_argument("--output", help="output jsonl file")
    parser.add_argument(
        "--proxystore",
        action="store_true",
        help="use proxystore",
    )
    parser.add_argument(
        "--repeat",
        default=5,
        type=int,
        help="number of times to repeat each run",
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        help="message sizes to try",
    )
    args = parser.parse_args()

    run(
        output=args.output,
        repeat=args.repeat,
        sizes=args.sizes,
        tag=args.tag,
        use_proxystore=args.proxystore,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
