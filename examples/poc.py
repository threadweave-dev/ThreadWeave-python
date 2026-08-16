import asyncio

from threadweave import ThreadWeave

tw = ThreadWeave(name="example", grpc_address="localhost:50051")


@tw.task
def foo():
    return "bar"


@tw.task
async def async_foo():
    await asyncio.sleep(10)
    return "bar"


if __name__ == "__main__":
    tw.client.connect()
    job = async_foo.submit()
    print(f"Job {job}")
    result = job.result()
    print(f"Result {result}")
