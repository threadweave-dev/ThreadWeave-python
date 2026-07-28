# ThreadWeave Python

> **The official Python runtime for ThreadWeave.**
>
> Write idiomatic Python.
> Execute anywhere.

---

> ⚠️ **Project status**
>
> ThreadWeave Python is currently in active design and early development.
>
> The API shown below represents the intended developer experience and may evolve before the first public release.

---

## Why?

Most distributed task queues were designed long before modern Python.

Today we write applications with:

* FastAPI
* async/await
* asyncio
* SQLAlchemy Async
* httpx
* AI workloads
* GPUs

Yet background execution often forces developers back into synchronous workers, complex concurrency models, or infrastructure-specific code.

ThreadWeave takes a different approach.

The execution engine is written in **Rust**.

The Python runtime focuses exclusively on providing a natural Python API.

Everything else—scheduling, retries, supervision, observability, distributed execution, resource allocation—is handled by the ThreadWeave engine.

---

# A familiar API

If you know FastAPI, this should feel natural.

```python
from threadweave import ThreadWeave

app = ThreadWeave()


@app.task
async def hello(name: str) -> str:
    return f"Hello {name}"
```

No worker configuration.

No broker-specific code.

No infrastructure inside your business logic.

---

# Native async

Async is not an afterthought.

It is a first-class citizen.

```python
@app.task
async def generate_embeddings(document_id: UUID):

    document = await database.documents.get(document_id)

    embedding = await embedding_model.embed(document.text)

    await database.embeddings.save(
        document.id,
        embedding,
    )
```

No event-loop hacks.

No synchronous wrappers.

No hidden thread pools.

---

# Resources instead of workers

Traditional task queues schedule work on workers.

ThreadWeave schedules work on **resources**.

```python
@app.task(
    resources={
        "cpu": 4,
        "memory": "8Gi",
        "gpu": 1,
    }
)
async def train_model(dataset: Dataset):
    ...
```

The application describes **what it needs**.

The scheduler decides **where it should run**.

---

# Retry policy

```python
@app.task(
    retries=5,
    retry_delay="30s",
)
async def send_email(message):
    ...
```

Retry strategies belong to the infrastructure.

Not to your business logic.

---

# Timeouts

```python
@app.task(
    timeout="10m",
)
async def process_video(video):
    ...
```

---

# Scheduling

```python
@app.task(
    schedule="0 * * * *",
)
async def cleanup():
    ...
```

Or

```python
await cleanup.delay()
```

Or

```python
await cleanup.delay(
    countdown="5m",
)
```

---

# Resource-aware AI workloads

AI jobs often require GPUs.

Instead of manually routing queues:

```python
@app.task(
    resources={
        "gpu": 2,
        "gpu_memory": "40Gi",
    }
)
async def run_llm(prompt):
    ...
```

The scheduler automatically selects a compatible node.

---

# Pipelines

Tasks compose naturally.

```python
document = await download(url)

text = await extract_text(document)

summary = await summarize(text)

await publish(summary)
```

Distributed execution should not force a different programming model.

---

# Events everywhere

Every execution emits structured events.

```text
TaskSubmitted

↓

TaskScheduled

↓

ResourcesAllocated

↓

TaskStarted

↓

TaskCompleted
```

Observability is built into the architecture.

Not added later.

---

# Compare with Celery

## Celery

```python
@app.task
def generate_pdf(document_id):

    document = load(document_id)

    pdf = build_pdf(document)

    save(pdf)
```

Scaling often requires thinking about:

* queues
* routing
* workers
* concurrency
* pools
* broker tuning

---

## ThreadWeave

```python
@app.task(
    resources={
        "cpu": 2,
        "memory": "4Gi",
    },
    timeout="5m",
)
async def generate_pdf(document_id):

    document = await repository.get(document_id)

    pdf = await renderer.render(document)

    await storage.save(pdf)
```

Business code stays focused on the business.

Infrastructure stays inside ThreadWeave.

---

# Long-running tasks

Minutes.

Hours.

Days.

ThreadWeave is designed to supervise long-running executions, survive failures, and recover from crashes without sacrificing observability.

---

# Built for extension

The Python runtime is only one runtime.

The ThreadWeave protocol allows additional runtimes to be implemented without changing the core engine.

Future runtimes may include:

* JavaScript / TypeScript
* Java
* Go
* .NET
* WebAssembly

---

# Philosophy

ThreadWeave is **not** a Python task queue.

It is a distributed execution platform whose first runtime happens to be Python.

Python developers get an idiomatic API.

The engine takes care of everything else.

---

# Current status

* RFCs in progress
* Architecture under design
* Rust core in development
* Python runtime specification in progress

Contributions, discussions and feedback are welcome.
