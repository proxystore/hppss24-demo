from __future__ import annotations

import argparse
import logging
import json
import random
import string
import sys
from collections import Counter
from typing import Sequence

import numpy
from dask.distributed import Client

logger = logging.getLogger("demo")


def generate_data(size: int) -> numpy.ndarray:
    return numpy.random.rand(size // 8)


def compute(data: numpy.ndarray) -> tuple[float, float]:
    mean = data.mean()
    stdev = data.std()
    return mean, stdev


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    logging.basicConfig(
        format="[%(asctime)s] %(levelname)-4s (%(name)s) :: %(message)s",
        level=logging.INFO,
    )

    client = Client(n_workers=args.workers)

    data = generate_data(args.size)
    logger.info(f"Generated data (size: {sys.getsizeof(data)/1e6:.3f} MB)")

    future = client.submit(compute, data)
    logger.info(f"Submitted compute task")

    mean, stdev = future.result()
    logger.info(f"Result: mean={mean:.2f}, stdev={stdev:.2f}")

    client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
