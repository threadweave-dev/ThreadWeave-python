import pytest

from threadweave import ThreadWeave
from threadweave.asyncio import ThreadWeave as AsyncThreadWeave


def test_threadweave_registers_a_task() -> None:
    app = ThreadWeave("example", namespace="tests")

    @app.task
    def add(left: int, right: int) -> int:
        return left + right

    assert app.qualified_name == "tests/example"
    assert app.get_task(add.id) is add
    assert add(2, 3) == 5


def test_threadweave_accepts_a_grpc_address() -> None:
    app = ThreadWeave("example", grpc_address="localhost:50051")

    assert app.client.endpoint == "localhost:50051"


def test_async_threadweave_accepts_a_grpc_address() -> None:
    app = AsyncThreadWeave("example", grpc_address="http://localhost:50051")

    assert app.client.endpoint == "http://localhost:50051"


def test_threadweave_rejects_legacy_endpoint() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'endpoint'"):
        ThreadWeave(
            "example",
            endpoint="localhost:50052",
        )
