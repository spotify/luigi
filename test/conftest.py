from typing import List

import pytest

import luigi.task_register


@pytest.fixture(autouse=True)
def reset_luigi_registry():
    """Reset the Luigi task registry before and after each test.

    Prevents registry pollution between tests when running with pytest-xdist,
    where multiple tests execute sequentially within the same worker process.
    This mirrors the behaviour of LuigiTestCase.setUp/tearDown and applies it
    to all tests automatically, including those that inherit unittest.TestCase
    directly without going through LuigiTestCase.
    """
    original = luigi.task_register.Register._get_reg()
    luigi.task_register.Register.clear_instance_cache()
    yield
    luigi.task_register.Register._set_reg(original)
    luigi.task_register.Register.clear_instance_cache()


def pytest_collection_modifyitems(items: List[pytest.Item]) -> None:
    """
    Automatically add the equivalent of pytest.mark.unmarked to any test which has no markers

    For example, enables the ability to target "contrib + unmarked" tests (eventually getting rid of the generic "contrib" marker):
      - pytest test/contrib/ -m "contrib or unmarked"
    """
    for item in items:
        # Check if the item has any markers (custom or builtin)
        if not any(item.iter_markers()):
            item.add_marker(pytest.mark.unmarked)
