# Resource reservation

Tasks can declare the compute resources they require and the capabilities that
must be provided by the worker executing them.

For example, the following inference task requires two CPU cores, 8 GiB of
system memory, one GPU, 24 GiB of GPU memory, and a worker providing CUDA and
PyTorch:

```python
import torch

from threadweave import Artifact, Resources, ThreadWeave


tw = ThreadWeave()


def load_model(file_like) -> torch.nn.Module:
    ...


@torch.no_grad()
def run_inference(
    model: Artifact,
    inputs: list[str],
) -> list[float]:
    device = torch.device(
        "cuda" if torch.cuda.is_available() else "cpu"
    )

    with model.open("rb") as f:
        loaded_model = load_model(f)

    loaded_model = loaded_model.to(device)

    if device.type == "cuda":
        model.mark_resident(
            scope=f"cuda:{torch.cuda.current_device()}"
        )
    else:
        model.mark_resident(
            scope="memory"
        )

    return loaded_model(inputs)


@tw.task(
    resources=Resources(
        cpu=2,
        memory="8GiB",
        gpu=1,
        gpu_memory="24GiB",
    ),
    capabilities=["cuda", "pytorch"],
)
def inference_task(
    *,
    model: Artifact,
    inputs: list[str],
) -> list[float]:
    return run_inference(
        model=model,
        inputs=inputs,
    )
```

Resource requirements and capabilities are hard placement constraints.

The scheduler only considers workers that can satisfy the requested resources
and provide all required capabilities. Among eligible workers, dynamic runtime
information such as Artifact locality, residency, current load, and expected
queue delay can then influence placement.

## Artifacts

Large inputs such as models, datasets, archives, or generated assets can be
represented as `Artifact` objects.

An Artifact represents the logical identity of immutable content. It can be
created from a local file before submitting a task:

```python
model = Artifact.from_file("./model.pt")

job = inference_task.submit(
    model=model,
    inputs=["test"],
)
```

The task signature remains explicit:

```python
def inference_task(
    *,
    model: Artifact,
    inputs: list[str],
):
    ...
```

ThreadWeave therefore does not need to overload task arguments with arbitrary
file-like objects. Artifact creation and task submission remain separate
operations with clear types.

This distinction is particularly useful when the source itself is
asynchronous. An asynchronous application may ingest a file asynchronously
while the task consuming the Artifact remains completely synchronous.

For example, an HTTP server may conceptually perform:

```python
artifact = await Artifact.from_async_file(upload)

job = inference_task.submit(
    model=artifact,
    inputs=["test"],
)
```

while the task still receives a normal `Artifact` and may expose its content
through synchronous file APIs.

Regardless of how it was created, the task always receives the same `Artifact`
abstraction.

## Artifact materialization

An Artifact is a logical object and does not imply that its content is already
available on the worker's local filesystem.

Task code can request a local file representation when required:

```python
with model.open("rb") as f:
    loaded_model = load_model(f)
```

ThreadWeave is responsible for making the Artifact available locally before
exposing the file handle.

Conceptually:

```text
Artifact
    │
    │ materialize
    ▼
Worker-local representation
    │
    │ open()
    ▼
Python file object
```

This allows ordinary synchronous Python libraries to consume Artifacts through
familiar file interfaces while ThreadWeave manages transport and
materialization underneath.

Materialization also creates useful locality information.

Once an Artifact has been materialized on a worker, accessing it again from
that worker may be significantly cheaper than transferring it to another
worker.

The runtime can expose this information to the scheduler without requiring the
application to manually describe where the Artifact is located.

## Artifact residency

Materialization is not the only form of locality that matters.

Some workloads transform an Artifact into a representation that is expensive
to construct but can be reused locally. A common example is a machine-learning
model loaded into memory or GPU memory.

After loading the model onto a device, task code can report that the Artifact
has a usable resident representation there:

```python
loaded_model = loaded_model.to(device)

model.mark_resident(
    scope=f"cuda:{torch.cuda.current_device()}"
)
```

Residency does not necessarily mean that the original Artifact bytes have been
copied unchanged to that location.

Instead, it means that a reusable representation associated with that Artifact
is currently available there.

For example:

```text
model.pt
    │
    │ materialize
    ▼
local file
    │
    │ torch.load()
    ▼
torch.nn.Module
    │
    │ .to("cuda:0")
    ▼
GPU-resident representation
```

The original Artifact provides the stable logical identity connecting these
representations.

An Artifact may therefore have several known residences simultaneously:

```text
Artifact sha256:abc...
│
├── worker-17 / filesystem
├── worker-17 / memory
└── worker-17 / cuda:0
```

Residency is dynamic runtime information rather than a static property of the
task declaration.

When a resident representation disappears, the corresponding residency should
also be invalidated or released so that the scheduler does not make placement
decisions using stale locality information.

## Residency-aware scheduling

Artifact residency can be used as an input to scheduling decisions.

Consider two otherwise equivalent workers:

```text
                         Worker A        Worker B

CPU available                yes             yes
Memory available             yes             yes
GPU available                yes             yes
CUDA capability              yes             yes
PyTorch capability           yes             yes

Model Artifact on GPU         no              yes
```

Both workers satisfy the task's hard resource and capability constraints.

Worker B, however, already has a usable representation of the model resident
on its GPU.

Executing the task there may therefore avoid transferring, loading, and
initializing the model again.

Residency does not make Worker B exclusively eligible for the task. Resources
and capabilities determine which workers are eligible to execute it.

Instead, residency contributes to the estimated cost of placing the task on
each eligible worker.

The scheduler considers not only the cost of preparing a worker for execution,
but also the expected time before that worker can actually start the task.

A worker where an Artifact is already resident may therefore be preferred when
it is immediately available, without becoming a permanent affinity target for
that Artifact.

## Cost-based placement

Consider a model that takes approximately 8 seconds to transfer, load, and
initialize on a GPU, while one inference takes approximately 1 second.

If Worker B already has the model resident and both workers are immediately
available:

```text
Worker A
    model initialization       8 s
    queue delay                0 s
    execution                  1 s
    -------------------------------
    estimated completion       9 s

Worker B
    model initialization       0 s
    queue delay                0 s
    execution                  1 s
    -------------------------------
    estimated completion       1 s
```

Worker B is the natural placement.

However, residency is not a hard affinity.

Suppose Worker B already has ten one-second tasks queued while Worker A is
idle:

```text
Worker A
    model initialization       8 s
    queue delay                0 s
    execution                  1 s
    -------------------------------
    estimated completion       9 s

Worker B
    model initialization       0 s
    queue delay               10 s
    execution                  1 s
    -------------------------------
    estimated completion      11 s
```

Waiting for the warm Worker B is now more expensive than paying the
initialization cost on Worker A.

The scheduler may therefore place the task on Worker A.

This distinction is fundamental: ThreadWeave does not attempt to maximize
locality. It attempts to minimize the expected cost of execution.

Conceptually:

```text
estimated completion cost
    =
    expected queue delay
    + preparation cost
    + execution cost
```

Preparation cost may include operations such as:

* transferring an Artifact to the worker;
* materializing it on local storage;
* loading data into memory;
* loading a model into GPU memory;
* constructing another reusable resident representation.

Residency reduces some of these costs, potentially to zero, but does not remove
the other terms from the scheduling decision.

## Dynamic residency expansion

Choosing Worker A in the previous example has another useful consequence.

Once Worker A has paid the initialization cost and loaded the model, it can
report a new residency for the same Artifact:

```text
Before:

    Worker A                         Worker B
    cuda:0                           cuda:0
      │                                │
      └── model absent                 └── model resident


After initialization on Worker A:

    Worker A                         Worker B
    cuda:0                           cuda:0
      │                                │
      └── model resident               └── model resident
```

Future tasks can now benefit from two warm workers.

This allows ThreadWeave to naturally spread workloads when demand becomes high
enough to justify the initialization cost.

At low load, repeatedly using the existing warm worker may be the cheapest
strategy:

```text
Low demand

Worker A                  Worker B
cold                      warm
                            ▲
                            │
                         tasks
```

As the queue on Worker B grows, its expected waiting cost increases.

Eventually, paying the initialization cost on Worker A becomes cheaper:

```text
Higher demand

Worker A                  Worker B
cold                      warm
  ▲                         ▲
  │                         │
new residency            existing residency
  │                         │
tasks                     tasks
```

Once Worker A becomes warm as well, subsequent tasks can be distributed across
both workers without paying that initialization cost again.

Residency can therefore produce an emergent form of workload expansion without
requiring the application to explicitly declare how many copies of an Artifact
should exist.

The scheduler continuously evaluates whether reusing an existing residency or
creating a new one provides the lower expected execution cost.

## Constraints, locality, and cost

ThreadWeave separates hard placement constraints from dynamic scheduling
signals.

```text
Resources + capabilities
        │
        ▼
Which workers CAN execute the task?
        │
        ▼
Eligible workers
        │
        ▼
Queue delay + locality + residency + preparation cost
        │
        ▼
Which worker SHOULD execute the task now?
```

Resources and capabilities determine eligibility.

Artifact locality and residency influence the cost of using each eligible
worker.

Current load and queue delay determine the cost of waiting for those workers.

The scheduler combines these signals when making a placement decision.

Artifact residency is therefore neither a placement constraint nor a strict
affinity.

It is a dynamic economic signal describing work that may already have been
performed at a particular location.

As cluster state and workload pressure change, the value of that locality
changes as well.

## Scheduling model

The resulting model can be summarized as:

* `Artifact` represents immutable logical content.
* **Materialization** creates or exposes a physical representation of that
  content.
* **Residency** records that a reusable representation associated with an
  Artifact is currently available at a particular location.
* **Resources** define the compute capacity required by a task.
* **Capabilities** define functionality that the executing worker must provide.
* **Resources and capabilities** are hard placement constraints.
* **Locality and residency** reduce preparation costs.
* **Queue state and worker load** contribute waiting costs.
* The **scheduler** compares eligible placements using their estimated execution
  cost.
* A warm worker is preferred only while waiting for it remains cheaper than
  preparing another eligible worker.
* Creating a new residency may itself reduce the cost of future placements.

This model allows ThreadWeave to exploit expensive cached state without turning
that state into a rigid affinity rule.

The scheduler can reuse warm resources under light load and progressively
expand work onto additional resources when queue pressure makes the
initialization cost worthwhile.
