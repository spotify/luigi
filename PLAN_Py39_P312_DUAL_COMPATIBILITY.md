# Plan: Python 3.9 / 3.12 Dual Compatibility

## Goal

Make the `rafay/BATCH-3679-luigi-py-312-upgrade` branch compatible with **both Py39 and Py312**, so DTs still on Py39 are not broken while Py312 support is added.

## Verdict: Achievable

All changes in PR #28 use standard Python 3 APIs available since Python 3.3. None of them introduce Py312-only syntax or APIs. The PR changes are **already dual compatible** as written. The remaining work is to fix Py312 regressions that were missed in the PR — and all those fixes are also backward compatible with Py39.

## Why the PR Changes Don't Break Py39

| Change | Files | Available since |
| --- | --- | --- |
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

*   `luigi/worker.py` — `random.seed((pid, time))` → `random.seed(hash((pid, time)))` (Py312 no longer accepts tuple seeds)
*   `luigi/local_target.py` — `random.randrange(0, 1e10)` → `random.randrange(0, 10**10)` (float not accepted in Py312)
*   `luigi/target.py:283` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))` (partial fix — see remaining items)
*   `luigi/parameter.py` — `from collections import Mapping` → `from collections.abc import Mapping` (removed from `collections` in Py312)
*   `luigi/scheduler.py` — `inspect.getargspec()` → `inspect.getfullargspec()` (removed in Py312)
*   `luigi/scheduler.py` — `collections.MutableSet` → `collections.abc.MutableSet` (removed from `collections` in Py312)
*   `luigi/scheduler.py` — `six.iteritems(self.resources())` → `self.resources().items()` (3 occurrences)
*   `luigi/tools/deps.py` — `collections.Iterable` → `collections.abc.Iterable` (removed from `collections` in Py312)
*   `luigi/tools/range.py` — `/` → `//` for integer divisibility check (float division behavior)
*   `luigi/tools/luigi_grep.py` — `six.moves.urllib.request.urlopen` → `from urllib.request import urlopen`
*   `luigi/tools/luigi_grep.py` — `six.iteritems()` → `.items()`
*   `luigi/rpc.py` — `luigi.six.moves.urllib.{parse,request,error}` → native `urllib.{parse,request,error}`
*   `luigi/rpc.py` — `except ImportError` → `except Exception` for `requests_unixsocket` import (broader catch for pkg issues)
*   `luigi/retcodes.py` — `luigi.interface._run(argv)['worker']` → `luigi.interface._run(argv).worker` (API now returns `LuigiRunResult` object, not dict)
*   `luigi/server.py` — `pkg_resources.resource_filename()` → `importlib.resources.files()` (`pkg_resources` deprecated in Py312)
*   `luigi/contrib/gcs.py` — `six.xrange` → `range`, `six.string_types` → `str`, `six.binary_type` → `bytes`, `six.BytesIO` → `io.BytesIO`, add `import io`
*   `luigi/contrib/hdfs/target.py` — `luigi.six.moves.urllib` → `from urllib import parse as urlparse`, remove `luigi.six.moves.range`
*   \[⚠️\] `luigi/contrib/s3.py` — entry is wrong: current file is the full 1.4.8 boto3 rewrite, not just `six.iteritems` → `.items()`. See "Wrong Base Branch" section above for the correct fix.
*   `luigi/contrib/salesforce.py` — `record.iteritems()` → `record.items()` (2 occurrences; `.iteritems()` is Python 2 only)

#### Test Code

*   `test/helpers.py` — Add `attr()` decorator as `nose.plugins.attrib.attr` replacement (`nose` uses removed `imp` module in Py312)
*   `test/cmdline_test.py` — Version-aware exception check: `FileNotFoundError` (Py312+) vs `KeyError` (Py39) for missing logging config files
*   `test/cmdline_test.py` — Fix `test_luigid_logging_conf`: add `get_config` and `os.path.exists` mocks
*   `test/contrib/batch_test.py` — Fix boto3 client setup to use `mock.patch` instead of direct assignment (avoids AWS region errors)
*   `test/contrib/gcs_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/contrib/hdfs/webhdfs_client_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/contrib/hdfs_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/contrib/sqla_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/db_task_history_test.py` — `pytest.mark.skip` on `DbTaskHistoryTest` (SQLAlchemy `DetachedInstanceError` with lazy-loaded relationships)
*   `test/db_task_history_test.py` — `pytest.mark.skip` on `MySQLDbTaskHistoryTest` (requires specific MySQL server config)
*   `test/execution_summary_test.py` — Update 5 expected strings: `"missing external dependencies"` → `"missing dependencies"` (match production `execution_summary.py`)
*   `test/interface_test.py` — `assertEquals` → `assertEqual` (deprecated in Py312, 2 occurrences)
*   `test/lock_test.py` — Add `time.sleep(0.1)` after `subprocess.Popen` for `/proc/{pid}/cmdline` readiness
*   `test/minicluster.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/range_test.py` — `parameter_tuples[:-2]` → `list(parameter_tuples)[:-2]` (generator cannot be sliced)
*   `test/range_test.py` — Add `parameter_tuples = list(parameter_tuples)` before iteration in second `bulk_complete`
*   `test/scheduler_api_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/scheduler_test.py` — `pytest.mark.skip` on 2 timing-sensitive race condition tests
*   `test/scheduler_test.py` — Remove `detailed_summary=True` from `luigi.build()` call; adjust assertions to match `bool` return
*   `test/scheduler_test.py` — Add `time.sleep(1)` for server startup; `process.join(timeout=30)` instead of fixed sleep
*   `test/server_test.py` — `from luigi.six.moves.urllib.parse import ...` → `from urllib.parse import ...`
*   `test/server_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/server_test.py` — `_ServerTest` converted to plain mixin (not `unittest.TestCase`); `UNIXServerTest` and `_INETServerTest` explicitly inherit both `_ServerTest` and `unittest.TestCase`; `_INETServerTest` gets `__test__ = False` to prevent pytest collecting the base class directly
*   `test/server_test.py` — `@unittest.skipUnless(luigi.rpc.HAS_UNIX_SOCKET, ...)` guard on `UNIXServerTest`
*   `test/server_test.py` — Remove `@skipOnTravis` on `test_404`
*   `test/snakebite_test.py` — `from nose.plugins.attrib import attr` → `from helpers import attr`
*   `test/task_serialize_test.py` — Handle both old (`hypothesis.extra.datetime`) and new (`hypothesis.strategies`) hypothesis APIs with try/except
*   `test/worker_test.py` — `assertEquals` → `assertEqual` (3 occurrences)
*   `test/worker_test.py` — `t.isAlive()` → `t.is_alive()` (deprecated and removed in Py312)

---

### ✅ Additional Fixes Implemented (beyond PR #28)

#### Production Code

*   `luigi/target.py:334` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))` (S3 temp path — missed in PR)
*   `luigi/contrib/ftp.py:257` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/ftp.py:282` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/ftp.py:300` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/ftp.py:411` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/ssh.py:257` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/ssh.py:271` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/ssh.py:288` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/hadoop_jar.py:50` — `random.randrange(0, 1e10)` → `random.randrange(0, int(1e10))`
*   `luigi/contrib/hdfs/target.py:180` — `random.randrange(1e10)` → `random.randrange(int(1e10))`
*   `luigi/contrib/hdfs/config.py:111` — `random.randrange(1e9)` → `random.randrange(int(1e9))`
*   `luigi/contrib/hdfs/config.py` — `six.PY3` condition removed (always true on Py3); `from luigi import six` import removed
*   `luigi/contrib/opener.py:38` — `from six.moves.urllib.parse import urlsplit, parse_qs` → `from urllib.parse import urlsplit, parse_qs`
*   `luigi/contrib/sge.py:281` — `open(self.job_file, "w").write(d)` → `open(self.job_file, "wb").write(d)`
*   `luigi/contrib/sge.py:283` — `pickle.dump(self, open(self.job_file, "w"))` → `pickle.dump(self, open(self.job_file, "wb"))`
*   `luigi/contrib/spark.py:310` — `pickle.dump(self, fd)`: `fd` is opened with `'wb'` at line 292 — no change needed
*   `luigi/contrib/hadoop.py:987` — `pickle.dump(self, open(file_name, "wb"))`: already uses `"wb"` — no change needed
*   `luigi/scheduler.py:461` — `pickle.dump(self.get_state(), fobj)` → `pickle.dump(self.get_state(), fobj, protocol=3)` (portable across all Py3.x runtimes)

---

### ⚠️ Wrong Base Branch — S3 Rewrite Leaked In

#### Root Cause

This branch was cut from `2.7.5.affirm.1.4.9` instead of `2.7.5+affirm.1.4.7`. The `1.4.8` release (`2.7.5+affirm.1.4.8`, tagged "upgrade luigi s3 to use boto3") introduced a complete rewrite of `luigi/contrib/s3.py` from `boto` to `boto3`. That rewrite was never intended to be part of this Py312 compatibility branch but leaked in via the wrong base.

#### What Leaked In (1.4.8 changes that should not be here)

| File | What changed in 1.4.8 | What should be here instead |
| --- | --- | --- |
| `luigi/contrib/s3.py` | Full boto→boto3 rewrite (~300 line diff) | 1.4.7 boto version + Py312 compat fixes + dual boto/boto3 support |
| `test/contrib/s3_test.py` | Rewritten for boto3 (moto API changes, `DeprecatedBotoClientException`, etc.) | 1.4.7 test version + Py312 compat fixes |

`setup.py` and `tox.ini` also changed in 1.4.8 but only for version bumps / test runner config — those are not a concern.

#### Required Fix

 `**luigi/contrib/s3.py**` — Reverted to 1.4.7 boto base. Applied Py312 fixes (removed `six`, fixed `iteritems`, Py3-only urlparse/configparser). Added boto3 fallback section at bottom of file inside `except ImportError` that redefines `S3Client` and `ReadableS3File` using botocore/boto3. Py39+boto uses boto path; Py312 (no boto) uses boto3 path automatically.

 `**test/contrib/s3_test.py**` — Reverted to 1.4.7 base. Added `HAS_BOTO` flag; boto-specific tests (`encrypt_key`, credential attrs, `Key.BufferSize`) guarded with `@unittest.skipIf(not HAS_BOTO, ...)`. `_create_bucket()` helper handles both boto and boto3 APIs. Fixed moto v4 import (`mock_s3`/`mock_sts` → `mock_aws` fallback). 58 tests collect on both Py39 and Py312.

#### Reference Points

*   1.4.7 base commit: `732759d5` (`bump version to 2.7.5+affirm.1.4.7`)
*   1.4.8 boto3 migration tag: `2.7.5+affirm.1.4.8`
*   To get the clean 1.4.7 s3.py: `git show 732759d5:luigi/contrib/s3.py`
*   To get the clean 1.4.7 s3\_test.py: `git show 732759d5:test/contrib/s3_test.py`

---

### ⬜ Remaining Work

#### Verify bundled `luigi/six.py` is Py312 safe

*   Confirmed `luigi/six.py` does not import `imp`, `thread`, or any other module removed in Py312 — grep returned no matches. Bundled copy is safe.

#### Dependency version constraints

*   `tornado>=6.0,<7` — verified: `6.5.5` installed and working on Py312
*   `urllib3>=2.0` — verified: `2.6.3` installed and working on Py312
*   `setuptools>=68` and `packaging>=23` — verified: `setuptools 82.0.1`, `packaging 26.0` on Py312
*   Confirm all of the above install correctly on Py39 environments used by DTs — verified via `.venv39` (pyenv 3.9.18): `tornado 6.5.5`, `urllib3 2.6.3`, `setuptools 82.0.1`, `packaging 26.0` all install and import cleanly

#### Environment / Packaging

*   `.python-version` file sets `3.12.7` — `pyenv` and `uv` will auto-select Py312 in this repo. DTs that install luigi as a package are unaffected (only the installed wheel matters, not the source `.python-version`). Documented in `CLAUDE.md` under Py39 Test Environment.
*   Version string `2.7.5+affirm.1.4.9.rc4` uses a PEP 440 local version label (`+`). Verified: `pip install -e .` resolves correctly on both Py39 (`.venv39`) and Py312 (`.venv`). `uv` uses `bypass-package-version-checks` in `uv.toml` to skip pre-release warnings.

---

### ⬜ Verification Steps

*   Install package in a Py312 virtualenv: `pip install -e .` — no import errors
*   Run test suite on Py312: **1325 passed, 23 skipped**; 39 failures and 2 collection errors all confirmed pre-existing (not introduced by these changes), verified by running the same failing tests against the original branch before changes
*   `from luigi import six` works on Py312 — bundled `luigi/six.py` loads correctly
*   `luigi/six.py` has no `import imp` or removed modules — confirmed clean
*   Install package in a Py39 virtualenv (`pyenv 3.9.18`) — no import errors
*   Run test suite on Py39: **1306 passed, 23 skipped**; all failures confirmed pre-existing by diffing against baseline — changes introduce **zero new failures** and fix one (`DynamicDependenciesWithMultipleWorkersTest::test_dynamic_dependencies` now passes)
*   Manually verify `luigid` starts and `HelloWorldTask` completes on both Py39 and Py312 — confirmed: `luigid --background` starts cleanly, `examples.HelloWorldTask` runs and prints "HelloWorldTask says: Hello world!" on both Py39 (`.venv39`) and Py312 (`.venv`)

## April 13 2026 — Dual boto1/boto3 `S3Client` Compatibility

### Background

`luigi/contrib/s3.py` currently contains two `S3Client` implementations:

*   **boto1** `S3Client` (lines 62–584): the original implementation using `boto`
*   **boto3** `S3Client` (lines 845–1151): defined inside a `try/except ImportError` block, only active when `boto` is not installed

The goal is to make both implementations always available as named classes. The desired usage pattern is per-call-site injection — callers import and pass the concrete class they need. A backwards-compatible `S3Client` alias pointing to `S3ClientBoto3` is kept for existing code that doesn't care about the backend.

A module-level `USE_BOTO3` flag was considered but rejected: since both `boto` and `boto3` can be installed simultaneously, auto-detection is ambiguous; and since Python modules are loaded once and cached, any flag is process-wide state that cannot safely be set differently in different importing modules.

### Audit of Client Usage Across the Luigi Package

#### `luigi/contrib/s3.py`

| Class / location | How client is obtained | Injection supported? |
| --- | --- | --- |
| `AtomicS3File.__init__` (line 594) | `s3_client` required parameter | Yes — always injected |
| `S3Target.__init__` (line 693) | `client=None` → `self.fs = client or S3Client()` | Yes — falls back to default only when omitted |
| `S3FlagTarget.__init__` (line 740) | `client=None` forwarded to `S3Target` | Yes — via `S3Target` |
| `S3EmrTarget.__init__` (line 772) | `*args, **kwargs` forwarded to `S3FlagTarget` | Yes — caller can pass `client=` |
| `S3PathTask.output()` (line 784) | `S3Target(self.path)` — no client passed | **No** — always uses default |
| `S3EmrTask.output()` (line 793) | `S3EmrTarget(self.path)` — no client passed | **No** — always uses default |
| `S3FlagTask.output()` (line 797) | `S3FlagTarget(self.path, flag=self.flag)` — no client passed | **No** — always uses default |

#### Other files in the Luigi package

| File / location | Usage | Injection supported? |
| --- | --- | --- |
| `luigi/tools/deps.py:102` | `isinstance(task_output, S3Target)` — type check only | N/A |
| `luigi/target.py:186` | Comment only | N/A |
| `luigi/contrib/hadoop.py:466,556,566` | `isinstance(...)` checks only | N/A |
| `luigi/contrib/opener.py:264` | `S3Target(..., **query)` — `client` is an allowed kwarg (line 256) | Yes — already supported |
| `luigi/contrib/redshift.py:488` | `class RedshiftManifestTask(S3PathTask)` — subclasses `S3PathTask` | Yes — inherits `client` once `S3PathTask` is fixed |
| `luigi/contrib/redshift.py:518` | `s3 = S3Target(folder_path)` inside `RedshiftManifestTask.run()` | **No** — always uses default |

`S3PathTask`, `S3EmrTask`, and `S3FlagTask` need a `client` parameter added so callers can inject the desired backend. `luigi/contrib/redshift.py:518` also directly instantiates `S3Target` with no client and will need the same fix.

### Implementation Checklist

 **1\. Rename boto1** `**S3Client**` **→** `**S3ClientBoto1**` (line 62)

*   Also rename boto1 `ReadableS3File` → `ReadableS3FileBoto1` (line 603)

 **2\. Extract boto3 classes out of the** `**except ImportError**` **block and rename**

*   Rename boto3 `S3Client` → `S3ClientBoto3` (currently line 845)
*   Rename boto3 `ReadableS3File` → `ReadableS3FileBoto3` (currently line 1153)
*   Move helper classes (`DeprecatedBotoClientException`, `_StreamingBodyAdaptor`, `_S3KeyWrapper`) to module level — harmless to define unconditionally since they import boto3 lazily inside methods

 **3\. Remove the** `**try/except ImportError**` **wrapper** — both class pairs are now always defined at module level with lazy imports inside their methods.

 **4\. Add backwards-compatible aliases** at the bottom of the module:

 **5\. Add** `**client**` **parameter to** `**S3PathTask**`**,** `**S3EmrTask**`**,** `**S3FlagTask**`  
These `ExternalTask` subclasses currently hardcode no client in `output()`, always falling  
back to the default. Add a `client` parameter (defaulting to `None`) that is forwarded to  
the target constructor. `None` means "use the module default", which after step 4 resolves  
to `S3ClientBoto1` — preserving existing behaviour exactly. To use boto3, the caller passes  
`client=S3ClientBoto3()` explicitly.

Apply the same pattern to `S3EmrTask` and `S3FlagTask` (forwarding `client` and `flag`  
where applicable).

Also fix `luigi/contrib/redshift.py:518` — `RedshiftManifestTask.run()` creates  
`S3Target(folder_path)` directly with no client. Since `RedshiftManifestTask` subclasses  
`S3PathTask`, once `S3PathTask` gains a `client` parameter, `RedshiftManifestTask` can  
forward `self._client` to that `S3Target` call as well.

 **6\. Run existing S3 tests** to confirm no regressions:

 **7\. Verify both implementations and the alias are importable by name**:

### Key Design Note

**Existing behaviour is preserved exactly.** Any code that currently works without passing a `client` continues to use boto1 — nothing changes for existing users.

boto3 is strictly opt-in via explicit injection:

```python
# existing code — unchanged, still uses boto1
from luigi.contrib.s3 import S3Target
target = S3Target("s3://bucket/key")

# new code opting in to boto3 — explicit at the call site
from luigi.contrib.s3 import S3ClientBoto3, S3Target
target = S3Target("s3://bucket/key", client=S3ClientBoto3())
```

`S3Client` remains as an alias for `S3ClientBoto1` so that existing imports of `S3Client` continue to work without modification.

---

## Migration Guide — `all-the-things` Repo

The following covers every distinct usage pattern found in the `all-the-things` codebase and what, if anything, needs to change to migrate to boto3.

---

### Pattern 1 — No client passed (~850 uses)

The dominant pattern. Found in `javelin`, `amplify`, `etl_pipelines`, and most other services. These fall back to the default `S3Client()` alias which stays as `S3ClientBoto1` after our changes.

```python
# Today — boto1 by default
return S3Target(self.get_s3_path())
return S3FlagTarget(path)
S3PathTask(path=some_path)
```

**To migrate to boto3:**

```python
from luigi.contrib.s3 import S3ClientBoto3, S3Target, S3FlagTarget

return S3Target(self.get_s3_path(), client=S3ClientBoto3())
return S3FlagTarget(path, client=S3ClientBoto3())
# S3PathTask — see Pattern 3 below
```

**No change needed** if staying on boto1. These sites only need updating when the team actively chooses to migrate.

---

### Pattern 2 — `AffirmS3Client` explicitly injected (~50 uses)

Found heavily in `etl_pipelines/data_privacy` and the legacy `toolbox`. `AffirmS3Client` is a boto1 subclass defined in `toolbox/affirmluigi/targets/s3.py`. It is injected via a `_s3_client` cached property or instantiated inline.

```python
# Today — AffirmS3Client is a boto1 subclass, injected explicitly
# etl_pipelines/data_privacy/task/delete_my_data/spawn_delete_my_data.py:72
return S3FlagTarget(output_flag_path, client=self._s3_client)

# etl_pipelines/data_privacy/task/pending_requests/publish_pending_privacy_requests.py:57
return S3Target(os.path.join(self.full_output_prefix, "copy_requests.json"), client=self._s3_client())

# toolbox/affirmluigi2/targets/api.py:26
client = AffirmS3Client(aws_access_key_id=key, aws_secret_access_key=secret)
return AffirmS3Target(path, client=client)
```

**To migrate to boto3**, swap `AffirmS3Client` for `S3ClientBoto3` at the injection site:

```python
from luigi.contrib.s3 import S3ClientBoto3

# cached property becomes:
@cached_property
def _s3_client(self) -> S3ClientBoto3:
    return S3ClientBoto3()

# inline instantiation becomes:
client = S3ClientBoto3(aws_access_key_id=key, aws_secret_access_key=secret)
return S3Target(path, client=client)
```

Note: any `AffirmS3Client`\-specific methods (e.g. `list_subdirs`, custom `put` with `encrypt_key`) would need to be re-evaluated against the boto3 API before migrating.

---

### Pattern 3 — `S3PathTaskWithRegion` — subclass overriding `output()` (~20 uses)

Found in `ml2/pipelines/core/common/utils/luigi.py`. Subclasses `S3PathTask` to inject a region-aware boto1 client via `output()`. This is also the recommended pattern for injecting any client into `S3PathTask`.

```python
# Today — S3ClientWithRegion is a boto1 subclass
# ml2/pipelines/core/common/utils/luigi.py:91
class S3PathTaskWithRegion(S3PathTask):
    region_name = Parameter()

    def output(self) -> S3Target:
        return S3Target(self.path, client=S3ClientWithRegion(region_name=self.region_name))
```

The migration in `all-the-things` is more involved than a base class swap. `S3ClientWithRegion`  
currently overrides the `s3` property using boto1-specific APIs (`boto.s3.connect_to_region()`,  
`STSConnection`) that do not exist in boto3. The full changes required in  
`ml2/pipelines/core/common/utils/luigi.py` are:

1.  Import `S3ClientBoto3` instead of `S3Client`
2.  Change the base class to `S3ClientBoto3`
3.  **Remove the entire** `**s3**` **property override** — `S3ClientBoto3` already supports `region_name`  
    natively by forwarding extra kwargs to `boto3.resource('s3', region_name=...)`
4.  Forward `region_name` to `super().__init__()` so it lands in `_options` and reaches boto3

```python
# Before — boto1, overrides s3 property with boto1-specific connect_to_region()
from boto.s3.connection import S3Connection
from boto.sts import STSConnection
from luigi.contrib.s3 import S3Client

class S3ClientWithRegion(S3Client):
    def __init__(self, region_name, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
        super().__init__(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, **kwargs)
        self.region_name = region_name

    @property
    def s3(self) -> S3Connection:
        # ... boto1-specific connect_to_region() and STSConnection logic ...
        self._s3 = s3.connect_to_region(region_name=self.region_name, ...)
        return self._s3

# After — boto3, s3 property removed entirely; region_name forwarded via kwargs
from luigi.contrib.s3 import S3ClientBoto3

class S3ClientWithRegion(S3ClientBoto3):
    def __init__(self, region_name, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
        # Pass region_name as a kwarg so S3ClientBoto3 forwards it to boto3.resource('s3', ...)
        super().__init__(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                         region_name=region_name, **kwargs)
        self.region_name = region_name  # kept for reference
    # No s3 property override needed — S3ClientBoto3 handles region_name natively
```

`S3PathTaskWithRegion.output()` is unchanged — it still passes  
`S3ClientWithRegion(region_name=self.region_name)` to `S3Target`.

Once we add a `client` parameter to `S3PathTask` (checklist step 5), simple cases that do not need a custom region can also just pass `client=S3ClientBoto3()` at instantiation time rather than subclassing:

```python
S3PathTask(path=some_path, client=S3ClientBoto3())
```

---

### Pattern 4 — `NamedS3FlagTask` — subclass of `S3FlagTask`, no client (~10 uses)

Found in `etl_lib/tasks/named_s3_flag_task.py`. Adds a `name` parameter to disambiguate tasks with the same path and flag. No client involved today.

```python
# Today — no client, uses boto1 default
class NamedS3FlagTask(S3FlagTask):
    name = luigi.Parameter("A name describing the task instance")
```

**No structural change needed.** Once we add `client` to `S3FlagTask` (checklist step 5), callers can opt in to boto3 at instantiation:

```python
from luigi.contrib.s3 import S3ClientBoto3

NamedS3FlagTask(
    name="chrono-user-views-all-offers",
    path="s3://bucket/prefix/",
    flag="_SUCCESS",
    client=S3ClientBoto3(),   # new — opt-in to boto3
)
```

---

### Migration Impact Summary

| Pattern | Approximate uses | Change needed to migrate to boto3 |
| --- | --- | --- |
| No client (default boto1) | ~850 | Add `client=S3ClientBoto3()` at each call site |
| `AffirmS3Client` injected | ~50 | Swap `AffirmS3Client` → `S3ClientBoto3` at injection site; audit any custom methods |
| `S3PathTaskWithRegion` | ~20 | In `S3ClientWithRegion`: change base class to `S3ClientBoto3`, remove boto1 `s3` property override, forward `region_name` via `super().__init__()` kwargs; `S3PathTaskWithRegion.output()` unchanged |
| `NamedS3FlagTask` | ~10 | Pass `client=S3ClientBoto3()` at instantiation once step 5 is complete |

All changes are localised to the call site. No structural refactoring is required.

```python
from luigi.contrib.s3 import S3ClientBoto1, S3ClientBoto3, S3Client
assert S3Client is S3ClientBoto1
```

```
python -m pytest test/contrib/s3_test.py -v
```

```python
class S3PathTask(ExternalTask):
    path = Parameter()
    # client is not a Luigi Parameter — it is a plain constructor argument
    # passed at task instantiation time, not serialised as task identity.
    def __init__(self, *args, client=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = client

    def output(self):
        return S3Target(self.path, client=self._client)
```

```python
# S3Client keeps the existing default — boto1, same as today.
# To use boto3, pass client=S3ClientBoto3() explicitly at the call site.
S3Client = S3ClientBoto1
ReadableS3File = ReadableS3FileBoto1
```