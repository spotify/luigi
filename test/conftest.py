import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """
    Automatically add the equivalent of pytest.mark.unmarked to any test which has no markers

    For example, enables the ability to target "contrib + unmarked" tests (eventually getting rid of the generic "contrib" marker):
      - pytest test/contrib/ -m "contrib or unmarked"
    """
    for item in items:
        # Check if the item has any markers (custom or builtin)
        if not any(item.iter_markers()):
            item.add_marker("unmarked")
