from __future__ import annotations

import argparse
import logging
import json
import sys
import time
import random
from typing import Any, Sequence

from dask.distributed import Client, as_completed
from proxystore.connectors.redis import RedisConnector
from proxystore.connectors.file import FileConnector
from proxystore.proxy import ProxyOr, Proxy, resolve
from proxystore.store import Store, get_store, register_store
from proxystore.store.utils import resolve_async

logger = logging.getLogger('scaling')


def warmup(store_config) -> None:
    store = Store.from_config(store_config)
    register_store(store, exist_ok=True)


def run_warmup(client: Client, tasks: int, store: Store) -> None:
    client.run(warmup, store.config())


def task(data: ProxyOr[bytes], sleep: float) -> tuple[ProxyOr[bytes], float]:
    start = time.perf_counter()
    if isinstance(data, Proxy):
        resolve_async(data)
    output = random.randbytes(len(data))
    if isinstance(data, Proxy):
        resolve(data)
        store = get_store(data)
        output = store.proxy(output)
    time.sleep(sleep)
    end = time.perf_counter()
    return output, end - start


def bag_of_tasks(
    client: Client,
    data_size_bytes: int,
    sleep: float,
    store: Store[Any],
    tasks: int,
    scatter: bool = False,
) -> float:
    start = time.perf_counter()

    elapsed_times = []
    futures = []

    submit_start = time.perf_counter()
    for _ in range(tasks):
        data = random.randbytes(data_size_bytes)
        if store is not None:
            data = store.proxy(data)
        if scatter:
            data = client.scatter(data)
        futures.append(client.submit(task, data, sleep, pure=False))
    logger.info(f'-- Submit duration {time.perf_counter() - submit_start:.3f}')

    wait_start = time.perf_counter()
    for future in as_completed(futures):
        data, elapsed = future.result()
        if isinstance(data, Proxy):
            resolve(data)
        elapsed_times.append(elapsed)
    logger.info(f'-- Wait/resolve duration {time.perf_counter() - wait_start:.3f}')

    logger.info(f'-- Avg. task time {sum(elapsed_times) / len(elapsed_times):.3f}')

    end = time.perf_counter()
    return end - start


def run(
    data_size_bytes: int,
    output: str,
    redis_addr: str | None,
    scheduler: str | None,
    sleep: float,
    tasks: int,
    workers_per_node: int,
    nodes: int,
) -> None:
    if scheduler is not None:
        client = Client(scheduler)
    else:
        client = Client(n_workers=workers_per_node)

    workers = nodes * workers_per_node
    logger.info(f'Waiting for {workers} workers')
    client.wait_for_workers(workers)
    logger.info('Workers ready')
    
    if redis_addr is not None:
        address, port = redis_addr.split(":")
        connector = RedisConnector(address, int(port))
    else:
        connector = FileConnector('proxystore-cache')

    with Store("scaling", connector, populate_target=True, register=True) as store:
        logger.info('Submitting warmup tasks')
        run_warmup(client, 2 * workers_per_node * nodes, store)
        logger.info('Warmup done')

        logger.info('Starting baseline')
        baseline_time = bag_of_tasks(client, data_size_bytes, sleep, None, tasks)
        logger.info(f'Baseline completed in {baseline_time:.3f}')
        
        logger.info('Starting ProxyStore')
        proxystore_time = bag_of_tasks(client, data_size_bytes, sleep, store, tasks)
        logger.info(f'ProxyStore completed in {proxystore_time:.3f}')

    workers = len(client.scheduler_info()["workers"])
    runs = [
        {
            "run": run,
            "workers": workers,
            "tasks": tasks,
            "runtime": runtime,
            "sleep": sleep,
            "data_size_bytes": data_size_bytes,
        }
        for run, runtime in zip(
            ('basline', 'proxystore'),
            (baseline_time, proxystore_time)
        )
    ]

    with open(output, "a") as f:
        for run in runs:
            f.write(json.dumps(run))
            f.write("\n")
    logger.info(f'Results written to {output}')

    client.close()


def main(argv: Sequence[str] | None = None) -> int:
    argv = sys.argv if argv is None else argv

    parser = argparse.ArgumentParser()
    parser.add_argument("--data-size-bytes", type=int)
    parser.add_argument("--output", help="output jsonl file")
    parser.add_argument("--redis-addr", default=None)
    parser.add_argument("--scheduler", default=None)
    parser.add_argument("--sleep", type=float)
    parser.add_argument("--tasks", type=int)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--nodes", type=int, default=1)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    run(
        data_size_bytes=args.data_size_bytes,
        output=args.output,
        redis_addr=args.redis_addr,
        scheduler=args.scheduler,
        sleep=args.sleep,
        tasks=args.tasks,
        workers_per_node=args.workers,
        nodes=args.nodes,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
