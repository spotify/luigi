# Plan: Python 3.9 / 3.12 Dual Compatibility

## Goal

Make the `rafay/BATCH-3679-luigi-py-312-upgrade` branch compatible with **both Py39 and Py312**, so DTs still on Py39 are not broken while Py312 support is added.

## Verdict: Achievable

All changes in PR #28 use standard Python 3 APIs available since Python 3.3. None of them introduce Py312-only syntax or APIs. The PR changes are **already dual compatible** as written. The remaining work is to fix Py312 regressions that were missed in the PR — and all those fixes are also backward compatible with Py39.

## Why the PR Changes Don't Break Py39

| Change | Files | Available since |
|---|---|---|
| `collections.abc.Mapping/MutableSet/Iterable` | `parameter.py`, `scheduler.py`, `deps.py` | Python 3.3 |
| `inspect.getfullargspec()` | `scheduler.py` | Python 3.0 |
| `random.seed(hash(tuple))` | `worker.py` | All Python versions |
| `randrange(0, 10**10)` int instead of float | `local_target.py`, `target.py` | All Python versions |
| Native `urllib.parse/request/error` | `rpc.py`, `luigi_grep.py` | Python 3.0 |
| `six.xrange` → `range`, `six.BytesIO` → `io.BytesIO`, etc. | `gcs.py`, `s3.py`, `hdfs/`, etc. | Python 3.0 |
| `importlib.resources.files` | `server.py` | Python 3.9 |
| `LuigiRunResult.worker` attribute access | `retcodes.py` | N/A — logic change |
| `tornado>=6.0,<7` | `setup.py` | Supports Python 3.6+ |
| `urllib3>=2.0` | `setup.py` | Supports Python 3.8+ |

---

## Exhaustive Checklist

### ✅ Already Done (PR #28) — Verified Dual Compatible

#### Production Code

- [x] `luigi/worker.py` — `random.seed((pid, time))` → `random.seed(hash((pid, time)))` (Py312 no longer accepts tuple seeds)
- [x] `luigi/local_target.py` — `random.randrange(0, 1e10)` → `random.randrange(0, 10**10)` (float not accepted in Py312)
- [x] `luigi/target.py:283` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))` (partial fix — see remaining items)
- [x] `luigi/parameter.py` — `from collections import Mapping` → `from collections.abc import Mapping` (removed from `collections` in Py312)
- [x] `luigi/scheduler.py` — `inspect.getargspec()` → `inspect.getfullargspec()` (removed in Py312)
- [x] `luigi/scheduler.py` — `collections.MutableSet` → `collections.abc.MutableSet` (removed from `collections` in Py312)
- [x] `luigi/scheduler.py` — `six.iteritems(self.resources())` → `self.resources().items()` (3 occurrences)
- [x] `luigi/tools/deps.py` — `collections.Iterable` → `collections.abc.Iterable` (removed from `collections` in Py312)
- [x] `luigi/tools/range.py` — `/` → `//` for integer divisibility check (float division behavior)
- [x] `luigi/tools/luigi_grep.py` — `six.moves.urllib.request.urlopen` → `from urllib.request import urlopen`
- [x] `luigi/tools/luigi_grep.py` — `six.iteritems()` → `.items()`
- [x] `luigi/rpc.py` — `luigi.six.moves.urllib.{parse,request,error}` → native `urllib.{parse,request,error}`
- [x] `luigi/rpc.py` — `except ImportError` → `except Exception` for `requests_unixsocket` import (broader catch for pkg issues)
- [x] `luigi/retcodes.py` — `luigi.interface._run(argv)['worker']` → `luigi.interface._run(argv).worker` (API now returns `LuigiRunResult` object, not dict)
- [x] `luigi/server.py` — `pkg_resources.resource_filename()` → `importlib.resources.files()` (`pkg_resources` deprecated in Py312)
- [x] `luigi/contrib/gcs.py` — `six.xrange` → `range`, `six.string_types` → `str`, `six.binary_type` → `bytes`, `six.BytesIO` → `io.BytesIO`, add `import io`
- [x] `luigi/contrib/hdfs/target.py` — `luigi.six.moves.urllib` → `from urllib import parse as urlparse`, remove `luigi.six.moves.range`
- [x] `luigi/contrib/s3.py` — `six.iteritems(config)` → `config.items()`, remove `from luigi import six`
- [x] `luigi/contrib/salesforce.py` — `record.iteritems()` → `record.items()` (2 occurrences; `.iteritems()` is Python 2 only)

#### Test Code

- [x] `test/helpers.py` — Add `attr()` decorator as `nose.plugins.attrib.attr` replacement (`nose` uses removed `imp` module in Py312)
- [x] `test/cmdline_test.py` — Version-aware exception check: `FileNotFoundError` (Py312+) vs `KeyError` (Py39) for missing logging config files
- [x] `test/cmdline_test.py` — Fix `test_luigid_logging_conf`: add `get_config` and `os.path.exists` mocks
- [x] `test/contrib/batch_test.py` — Fix boto3 client setup to use `mock.patch` instead of direct assignment (avoids AWS region errors)
- [x] `test/contrib/gcs_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/contrib/hdfs/webhdfs_client_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/contrib/hdfs_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/contrib/sqla_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/db_task_history_test.py` — `pytest.mark.skip` on `DbTaskHistoryTest` (SQLAlchemy `DetachedInstanceError` with lazy-loaded relationships)
- [x] `test/db_task_history_test.py` — `pytest.mark.skip` on `MySQLDbTaskHistoryTest` (requires specific MySQL server config)
- [x] `test/execution_summary_test.py` — Update 5 expected strings: `"missing external dependencies"` → `"missing dependencies"` (match production `execution_summary.py`)
- [x] `test/interface_test.py` — `assertEquals` → `assertEqual` (deprecated in Py312, 2 occurrences)
- [x] `test/lock_test.py` — Add `time.sleep(0.1)` after `subprocess.Popen` for `/proc/{pid}/cmdline` readiness
- [x] `test/minicluster.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/range_test.py` — `parameter_tuples[:-2]` → `list(parameter_tuples)[:-2]` (generator cannot be sliced)
- [x] `test/range_test.py` — Add `parameter_tuples = list(parameter_tuples)` before iteration in second `bulk_complete`
- [x] `test/scheduler_api_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/scheduler_test.py` — `pytest.mark.skip` on 2 timing-sensitive race condition tests
- [x] `test/scheduler_test.py` — Remove `detailed_summary=True` from `luigi.build()` call; adjust assertions to match `bool` return
- [x] `test/scheduler_test.py` — Add `time.sleep(1)` for server startup; `process.join(timeout=30)` instead of fixed sleep
- [x] `test/server_test.py` — `from luigi.six.moves.urllib.parse import ...` → `from urllib.parse import ...`
- [x] `test/server_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/server_test.py` — `_ServerTest` converted to plain mixin (not `unittest.TestCase`); `UNIXServerTest` and `_INETServerTest` explicitly inherit both `_ServerTest` and `unittest.TestCase`; `_INETServerTest` gets `__test__ = False` to prevent pytest collecting the base class directly
- [x] `test/server_test.py` — `@unittest.skipUnless(luigi.rpc.HAS_UNIX_SOCKET, ...)` guard on `UNIXServerTest`
- [x] `test/server_test.py` — Remove `@skipOnTravis` on `test_404`
- [x] `test/snakebite_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
- [x] `test/task_serialize_test.py` — Handle both old (`hypothesis.extra.datetime`) and new (`hypothesis.strategies`) hypothesis APIs with try/except
- [x] `test/worker_test.py` — `assertEquals` → `assertEqual` (3 occurrences)
- [x] `test/worker_test.py` — `t.isAlive()` → `t.is_alive()` (deprecated and removed in Py312)

---

### ✅ Additional Fixes Implemented (beyond PR #28)

#### Production Code

- [x] `luigi/target.py:334` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))` (S3 temp path — missed in PR)
- [x] `luigi/contrib/ftp.py:257` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/ftp.py:282` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/ftp.py:300` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/ftp.py:411` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/ssh.py:257` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/ssh.py:271` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/ssh.py:288` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/hadoop_jar.py:50` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
- [x] `luigi/contrib/hdfs/target.py:180` — `random.randrange(1e10)` → `random.randrange(int(1e10))`
- [x] `luigi/contrib/hdfs/config.py:111` — `random.randrange(1e9)` → `random.randrange(int(1e9))`
- [x] `luigi/contrib/hdfs/config.py` — `six.PY3` condition removed (always true on Py3); `from luigi import six` import removed
- [x] `luigi/contrib/opener.py:38` — `from six.moves.urllib.parse import urlsplit, parse_qs` → `from urllib.parse import urlsplit, parse_qs`
- [x] `luigi/contrib/sge.py:281` — `open(self.job_file, "w").write(d)` → `open(self.job_file, "wb").write(d)`
- [x] `luigi/contrib/sge.py:283` — `pickle.dump(self, open(self.job_file, "w"))` → `pickle.dump(self, open(self.job_file, "wb"))`
- [x] `luigi/contrib/spark.py:310` — `pickle.dump(self, fd)`: `fd` is opened with `'wb'` at line 292 — no change needed
- [x] `luigi/contrib/hadoop.py:987` — `pickle.dump(self, open(file_name, "wb"))`: already uses `"wb"` — no change needed
- [x] `luigi/scheduler.py:461` — `pickle.dump(self.get_state(), fobj)` → `pickle.dump(self.get_state(), fobj, protocol=3)` (portable across all Py3.x runtimes)

---

### ⬜ Remaining Work

#### Verify bundled `luigi/six.py` is Py312 safe

- [x] Confirmed `luigi/six.py` does not import `imp`, `thread`, or any other module removed in Py312 — grep returned no matches. Bundled copy is safe.

#### Dependency version constraints

- [x] `tornado>=6.0,<7` — verified: `6.5.5` installed and working on Py312
- [x] `urllib3>=2.0` — verified: `2.6.3` installed and working on Py312
- [x] `setuptools>=68` and `packaging>=23` — verified: `setuptools 82.0.1`, `packaging 26.0` on Py312
- [x] Confirm all of the above install correctly on Py39 environments used by DTs — verified via `.venv39` (pyenv 3.9.18): `tornado 6.5.5`, `urllib3 2.6.3`, `setuptools 82.0.1`, `packaging 26.0` all install and import cleanly

#### Environment / Packaging

- [x] `.python-version` file sets `3.12.7` — `pyenv` and `uv` will auto-select Py312 in this repo. DTs that install luigi as a package are unaffected (only the installed wheel matters, not the source `.python-version`). Documented in `CLAUDE.md` under Py39 Test Environment.
- [x] Version string `2.7.5+affirm.1.4.9.rc4` uses a PEP 440 local version label (`+`). Verified: `pip install -e .` resolves correctly on both Py39 (`.venv39`) and Py312 (`.venv`). `uv` uses `bypass-package-version-checks` in `uv.toml` to skip pre-release warnings.

---

### ⬜ Verification Steps

- [x] Install package in a Py312 virtualenv: `pip install -e .` — no import errors
- [x] Run test suite on Py312: **1325 passed, 23 skipped**; 39 failures and 2 collection errors all confirmed pre-existing (not introduced by these changes), verified by running the same failing tests against the original branch before changes
- [x] `from luigi import six` works on Py312 — bundled `luigi/six.py` loads correctly
- [x] `luigi/six.py` has no `import imp` or removed modules — confirmed clean
- [x] Install package in a Py39 virtualenv (`pyenv 3.9.18`) — no import errors
- [x] Run test suite on Py39: **1306 passed, 23 skipped**; all failures confirmed pre-existing by diffing against baseline — changes introduce **zero new failures** and fix one (`DynamicDependenciesWithMultipleWorkersTest::test_dynamic_dependencies` now passes)
- [x] Manually verify `luigid` starts and `HelloWorldTask` completes on both Py39 and Py312 — confirmed: `luigid --background` starts cleanly, `examples.HelloWorldTask` runs and prints "HelloWorldTask says: Hello world!" on both Py39 (`.venv39`) and Py312 (`.venv`)
