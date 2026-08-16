import asyncio
import time

from threadweave.asyncio import ThreadWeave

tw = ThreadWeave(name="example", grpc_address="localhost:50051")


@tw.task
def foo():
    time.sleep(2)
    return "bar"


@tw.task
async def async_foo():
    await asyncio.sleep(10)
    return "bar"


async def main():
    await tw.client.connect()
    job = await foo.submit()
    print(f"Job {job}")
    result = await job.result()
    print(f"Result {result}")


if __name__ == "__main__":
    asyncio.run(main())
