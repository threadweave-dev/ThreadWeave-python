import inspect
import re
from collections.abc import Callable, Iterator, Mapping
from typing import Any, Generic, TypeVar

from threadweave._internal.registry import TaskRegistry
from threadweave._internal.task import BaseTask

TaskT = TypeVar("TaskT", bound="BaseTask[Any, Any]")

_APPLICATION_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")
_NAMESPACE_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.-]*$")


class BaseThreadWeave(Generic[TaskT]):
    """
    Common base class for ThreadWeave application implementations.

    `BaseThreadWeave` contains the state and behavior shared by the
    synchronous and asynchronous ThreadWeave APIs.

    It is intentionally transport-agnostic and execution-model-agnostic.
    In particular, this class does not define how the application
    communicates with the ThreadWeave Core and does not assume whether
    operations are synchronous or asynchronous.

    Concrete implementations are responsible for providing the appropriate
    protocol client and task type.

    Examples
    --------
    The synchronous API may specialize this class with its synchronous Task:

        class ThreadWeave(BaseThreadWeave[Task]):
            ...

    while the asyncio API may specialize it with its asynchronous Task:

        class ThreadWeave(BaseThreadWeave[AsyncTask]):
            ...

    Parameters
    ----------
    name:
        Application name.

        The name forms part of the canonical identifier of every task
        registered by this application.

    namespace:
        Namespace containing the application.

        Namespaces allow applications with identical names to coexist while
        retaining globally unambiguous task identifiers.

    default_queue:
        Default queue used for tasks that do not explicitly declare one.

    default_resources:
        Default resource requirements inherited by registered tasks.

        Task-level resource declarations may extend or override these values.

    default_capabilities:
        Default capabilities required by tasks registered by this application.

        Task-level capabilities are combined with these application defaults.

    Notes
    -----
    This class owns only application-level metadata, task registration state,
    and behavior that is identical between the synchronous and asynchronous
    APIs.

    I/O operations such as connecting to the ThreadWeave Core, submitting jobs,
    waiting for results, or closing protocol clients belong to concrete
    implementations.

    The generic ``TaskT`` parameter represents the concrete Task type produced
    and exposed by a particular ThreadWeave implementation.
    """

    def __init__(
        self,
        name: str,
        *,
        namespace: str = "default",
        default_queue: str | None = None,
        default_resources: Mapping[str, Any] | None = None,
        default_capabilities: tuple[str, ...] = (),
    ) -> None:
        self._name = self._validate_application_name(name)
        self._namespace = self._validate_namespace(namespace)

        self._default_queue = default_queue
        self._default_resources = dict(default_resources or {})
        self._default_capabilities = tuple(default_capabilities)

        self._registry: TaskRegistry[TaskT] = TaskRegistry()

    @staticmethod
    def _validate_application_name(name: str) -> str:
        name = name.strip()
        if not name:
            raise ValueError("Application name cannot be empty.")
        if not _APPLICATION_NAME_PATTERN.fullmatch(name):
            raise ValueError(
                "Application name must start with a letter and contain "
                "only letters, digits, underscores, or hyphens."
            )
        return name

    @staticmethod
    def _validate_namespace(namespace: str) -> str:
        namespace = namespace.strip()
        if not namespace:
            raise ValueError("Namespace cannot be empty.")
        if not _NAMESPACE_PATTERN.fullmatch(namespace):
            raise ValueError(
                "Namespace must start with a letter and contain only "
                "letters, digits, dots, underscores, or hyphens."
            )
        return namespace

    @property
    def name(self) -> str:
        """
        Return the application name.

        The application name participates in the canonical identifier of
        registered tasks.
        """
        return self._name

    @property
    def namespace(self) -> str:
        """
        Return the namespace containing the application.
        """
        return self._namespace

    @property
    def qualified_name(self) -> str:
        """
        Return the canonical namespace/application identifier.

        Returns
        -------
        str
            Identifier in the form ``"<namespace>/<application>"``.
        """
        return f"{self._namespace}/{self._name}"

    @property
    def registry(self) -> TaskRegistry[TaskT]:
        """
        Return the local task registry owned by this application.

        The registry contains tasks discovered or explicitly registered in the
        current Python process. It does not represent remote scheduler state.
        """
        return self._registry

    def get_task(self, name: str) -> TaskT:
        """
        Return a registered task by name.

        Parameters
        ----------
        name:
            Local or canonical task name understood by the registry.

        Returns
        -------
        TaskT
            The concrete Task implementation associated with this application.

        Raises
        ------
        KeyError
            If no registered task matches ``name``.
        """
        return self._registry[name]

    def iter_tasks(self) -> Iterator[TaskT]:
        """
        Iterate over all tasks registered by this application.

        The iterator reflects the current contents of the local task registry.
        """
        return iter(self._registry.snapshot().values())

    def discover_tasks(self) -> tuple[TaskT, ...]:
        """
        Return an immutable snapshot of all registered tasks.

        This method is primarily intended for runtime discovery and worker
        startup, where a stable collection of the application's currently
        registered tasks is required.
        """
        return tuple(self._registry.snapshot().values())

    def _build_task_id(self, task_name: str) -> str:
        task_name = task_name.strip()
        if not task_name:
            raise ValueError("Task name cannot be empty.")
        if task_name.startswith(".") or task_name.endswith("."):
            raise ValueError(
                f"Invalid task name {task_name!r}: "
                "a task name cannot start or end with a dot."
            )
        return f"{self._namespace}.{self._name}.{task_name}"

    @staticmethod
    def _validate_function(function: Callable[..., Any]) -> None:
        if not inspect.isfunction(function):
            raise TypeError("A ThreadWeave Task must decorate a Python function.")
        if inspect.isgeneratorfunction(function):
            raise TypeError(
                "Generator functions are not supported as ThreadWeave Tasks."
            )
        if inspect.isasyncgenfunction(function):
            raise TypeError(
                "Async generator functions are not supported as ThreadWeave Tasks."
            )

    @staticmethod
    def _validate_execution_options(
        *, retries: int, timeout: float | None
    ) -> None:
        if isinstance(retries, bool) or not isinstance(retries, int):
            raise TypeError("retries must be an integer.")
        if retries < 0:
            raise ValueError("retries cannot be negative.")
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise TypeError("timeout must be a number of seconds.")
            if timeout <= 0:
                raise ValueError("timeout must be greater than zero.")

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"name={self._name!r}, "
            f"namespace={self._namespace!r}, "
            f"tasks={len(self._registry)}"
            f")"
        )
