# -*- coding: utf-8 -*-
#
# Copyright 2024 Affirm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
"""Tests for ``luigi.contrib._luigi1_compat.IS_LUIGI1_DEPRECATED``.

The flag's contract:
* ``True``  — ``import luigi1`` raised (any ``Exception``).
* ``False`` — ``luigi1`` imported cleanly.

These tests exercise all three branches of that contract: importable,
unimportable (``ImportError``), and importable-but-raises.
"""

import importlib
import sys
import types

from helpers import unittest


COMPAT_MODULE = 'luigi.contrib._luigi1_compat'


def _force_reimport_compat():
    """Drop and re-import the compat module so its top-level ``import luigi1``
    re-runs against the current ``sys.modules`` / ``sys.meta_path`` state."""
    sys.modules.pop(COMPAT_MODULE, None)
    return importlib.import_module(COMPAT_MODULE)


class _RaiseOnImportFinder:
    """A ``sys.meta_path`` finder that raises a chosen exception when a
    specific module name is imported. Used to simulate ``luigi1`` raising
    something other than ``ImportError`` at import time."""

    def __init__(self, target_name, exc):
        self.target_name = target_name
        self.exc = exc

    def find_spec(self, fullname, path, target=None):
        if fullname == self.target_name:
            raise self.exc
        return None


class TestLuigi1CompatFlag(unittest.TestCase):

    def setUp(self):
        # Snapshot the modules we may mutate so we can restore cleanly.
        self._saved_luigi1 = sys.modules.get('luigi1', None)
        self._had_luigi1 = 'luigi1' in sys.modules
        self._saved_meta_path = list(sys.meta_path)

    def tearDown(self):
        # Restore module + meta_path state.
        if self._had_luigi1:
            sys.modules['luigi1'] = self._saved_luigi1
        else:
            sys.modules.pop('luigi1', None)
        sys.meta_path[:] = self._saved_meta_path
        # Re-import the compat module so the rest of the test suite sees the
        # real flag value for this environment.
        _force_reimport_compat()

    def test_flag_is_bool(self):
        compat = importlib.import_module(COMPAT_MODULE)
        self.assertIsInstance(compat.IS_LUIGI1_DEPRECATED, bool)

    def test_flag_true_when_luigi1_unimportable(self):
        # In the dev/CI env luigi1 is not installed. Make sure we evaluate the
        # flag against that clean state.
        sys.modules.pop('luigi1', None)
        compat = _force_reimport_compat()
        self.assertTrue(
            compat.IS_LUIGI1_DEPRECATED,
            "expected IS_LUIGI1_DEPRECATED == True when luigi1 cannot be imported",
        )

    def test_flag_false_when_luigi1_importable(self):
        # Stub luigi1 so the top-level ``import luigi1`` in the compat module
        # succeeds without finding the real package.
        sys.modules['luigi1'] = types.ModuleType('luigi1')
        compat = _force_reimport_compat()
        self.assertFalse(
            compat.IS_LUIGI1_DEPRECATED,
            "expected IS_LUIGI1_DEPRECATED == False when luigi1 imports cleanly",
        )

    def test_flag_true_when_luigi1_import_raises_non_importerror(self):
        # The compat module catches ``Exception``, not just ``ImportError``,
        # so an arbitrary import-time failure inside luigi1 should still
        # flip the flag to True.
        sys.modules.pop('luigi1', None)
        finder = _RaiseOnImportFinder('luigi1', RuntimeError('luigi1 init blew up'))
        sys.meta_path.insert(0, finder)
        try:
            compat = _force_reimport_compat()
        finally:
            # tearDown also resets sys.meta_path, but we want any reload below
            # in this test to use the original list.
            sys.meta_path.remove(finder)
        self.assertTrue(
            compat.IS_LUIGI1_DEPRECATED,
            "expected IS_LUIGI1_DEPRECATED == True when import luigi1 raises a non-ImportError",
        )


if __name__ == '__main__':
    unittest.main()
