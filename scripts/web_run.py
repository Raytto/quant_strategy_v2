from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
import uvicorn

from qs.web.app import create_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QS web app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    uvicorn.run(create_app(), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
