# -*- coding: utf-8 -*-
#
# Copyright 2017 VNG Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
luigi.s3 has moved to :py:mod:`luigi.contrib.s3`
"""
# Delete this file any time after 24 march 2017

import warnings

from luigi.contrib.s3 import *  # NOQA
warnings.warn("luigi.s3 module has been moved to luigi.contrib.s3",
              DeprecationWarning)
