from threadweave import ThreadWeave


def test_threadweave_registers_a_task() -> None:
    app = ThreadWeave("example", namespace="tests")

    @app.task
    def add(left: int, right: int) -> int:
        return left + right

    assert app.qualified_name == "tests/example"
    assert app.get_task(add.id) is add
    assert add(2, 3) == 5
