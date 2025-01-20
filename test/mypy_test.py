import sys
import tempfile
import unittest

from mypy import api


class TestMyMypyPlugin(unittest.TestCase):
    def test_plugin_no_issue(self):
        if sys.version_info[:2] < (3, 8):
            return

        test_code = """
import luigi
from uuid import UUID


class UUIDParameter(luigi.Parameter):
    def parse(self, s):
        return UUID(s)


class MyTask(luigi.Task):
    foo: int = luigi.IntParameter()
    bar: str = luigi.Parameter()
    uniq: UUID = UUIDParameter()
    baz: str = luigi.Parameter(default="baz")

MyTask(foo=1, bar='bar', uniq=UUID("9b0591d7-a167-4978-bc6d-41f7d84a288c"))
"""

        with tempfile.NamedTemporaryFile(suffix=".py") as test_file:
            test_file.write(test_code.encode("utf-8"))
            test_file.flush()
            result = api.run(
                [
                    "--no-incremental",
                    "--cache-dir=/dev/null",
                    "--config-file",
                    "test/testconfig/pyproject.toml",
                    test_file.name,
                ]
            )
            self.assertIn("Success: no issues found", result[0])

    def test_plugin_invalid_arg(self):
        if sys.version_info[:2] < (3, 8):
            return

        test_code = """
import luigi


class MyTask(luigi.Task):
    foo: int = luigi.IntParameter()
    bar: str = luigi.Parameter()
    baz: str = luigi.Parameter(default=1) # invalid assignment to str with default value int

# issue:
#   - foo is int
#   - unknown is unknown parameter
#   - baz is invalid assignment to str with default value int
MyTask(foo='1', bar="bar", unknown="unknown")
        """

        with tempfile.NamedTemporaryFile(suffix=".py") as test_file:
            test_file.write(test_code.encode("utf-8"))
            test_file.flush()
            result = api.run(
                [
                    "--no-incremental",
                    "--cache-dir=/dev/null",
                    "--config-file",
                    "test/testconfig/pyproject.toml",
                    test_file.name,
                ]
            )

            self.assertIn(
                'error: Incompatible types in assignment (expression has type "int", variable has type "str")  [assignment]',
                result[0],
            )  # check default value assignment
            self.assertIn(
                'error: Argument "foo" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]',
                result[0],
            )  # check foo argument
            self.assertIn(
                'error: Unexpected keyword argument "unknown" for "MyTask"  [call-arg]',
                result[0],
            )  # check unknown argument
            self.assertIn("Found 3 errors in 1 file (checked 1 source file)", result[0])
