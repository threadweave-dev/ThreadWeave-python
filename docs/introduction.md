# Introduction

ThreadWeave brings distributed task execution to Python without making your
application feel distributed. Define ordinary synchronous or asynchronous
functions, decorate them as tasks, and let the ThreadWeave runtime take care of
where and how they run.

## Installation

Install the Python package:

```bash
pip install threadweave-python
```

To include the precompiled Rust client, install the `binary` extra:

```bash
pip install "threadweave-python[binary]"
```

## Define a task

### In a synchronous application

If you have used Celery, the synchronous API should feel immediately familiar:
create an application, decorate a function as a task, then submit it when work
needs to move into the background. Create a `ThreadWeave` application and use
`@tw.task()` to turn any function into a task:

```python
from threadweave import ThreadWeave

tw = ThreadWeave()


@tw.task()
def add(numbers: list[int]):
    return sum(numbers)
```

Tasks may also be asynchronous. The executor understands `async def` natively
and runs the coroutine in the appropriate environment:

```python
@tw.task()
async def async_task(*args):
    await something()
```

### In an asynchronous application

For an async-native client, import `ThreadWeave` from `threadweave.asyncio`:

```python
from threadweave.asyncio import ThreadWeave

tw = ThreadWeave()
```

Task definitions stay exactly the same. Only operations that communicate with
the underlying client become awaitable.

## Submit a task

### From synchronous code

The synchronous API blocks when it communicates with the client, so it fits
naturally into applications that do not run an event loop. This does not limit
your tasks: asynchronous functions are still executed by the worker.

```python
job = add.submit([1, 2, 3])
result = job.result(timeout=10)

print(result)
# 6
```

### From asynchronous code

The asynchronous API keeps every client operation awaitable—even when the task
itself is CPU-bound—so submitting work never blocks your application's event
loop.

```python
job = await add.submit([1, 2, 3])
result = await job.result(timeout=10)

print(result)
# 6
```
