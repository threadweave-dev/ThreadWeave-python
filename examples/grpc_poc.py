from __future__ import annotations

import argparse
from pathlib import Path

from threadweave.core_process import CoreProcess
from threadweave.grpc_client import GrpcClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("core_executable", type=Path)
    args = parser.parse_args()

    with CoreProcess(args.core_executable) as core:
        assert core.endpoint is not None
        print(f"Core ready at {core.endpoint.address}")
        with GrpcClient(core.endpoint.address) as client:
            result = client.submit_job(
                namespace="development",
                application="demo",
                task="demo.add",
                args=(1, 2),
                kwargs={},
                metadata={"source": "python-poc"},
            )
            print(f"Job accepted: id={result.job_id}, state={result.state}")


if __name__ == "__main__":
    main()
