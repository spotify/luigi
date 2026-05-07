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
"""
Private compatibility flag used by ``luigi.contrib`` to switch behaviour
between the legacy boto1-based stack (used while the ``luigi1`` package
is still installed alongside this one) and the modern boto3-based stack
(used once ``luigi1`` has been removed from the environment).

The single public name in this module is :data:`IS_LUIGI1_DEPRECATED`:

* ``False`` — ``luigi1`` is importable. Callers should keep using the
  legacy boto1 implementations (``S3ClientBoto1``, ``ReadableS3FileBoto1``,
  ...) for backwards compatibility with the legacy stack.
* ``True``  — ``luigi1`` is no longer available (its import raised). Callers
  should use the modern boto3 implementations.

This module is intentionally private (leading underscore): it exists to
gate internal dispatch in ``luigi.contrib`` and is not part of the public
API.
"""

try:
    import luigi1  # noqa: F401
    IS_LUIGI1_DEPRECATED = False
except Exception:
    # Any import-time failure (ImportError, or an error raised inside
    # luigi1 itself) means the legacy stack is no longer usable.
    IS_LUIGI1_DEPRECATED = True
