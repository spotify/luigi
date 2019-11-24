# -*- coding: utf-8 -*-
#
# Copyright 2017 Spotify AB.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import datetime
import logging

import luigi

logger = logging.getLogger('luigi-interface')


class ExternalDailySnapshot(luigi.ExternalTask):
    """
    Abstract class containing a helper method to fetch the latest snapshot.

    Example::

      class MyTask(luigi.Task):
        def requires(self):
          return PlaylistContent.latest()

    All tasks subclassing :class:`ExternalDailySnapshot` must have a :class:`luigi.DateParameter`
    named ``date``.

    You can also provide additional parameters to the class and also configure
    lookback size.

    Example::

      ServiceLogs.latest(service="radio", lookback=21)

    """
    date = luigi.DateParameter()
    __cache = []

    @classmethod
    def latest(cls, *args, **kwargs):
        """This is cached so that requires() is deterministic."""
        date = kwargs.pop("date", datetime.date.today())
        lookback = kwargs.pop("lookback", 14)
        # hashing kwargs deterministically would be hard. Let's just lookup by equality
        key = (cls, args, kwargs, lookback, date)
        for k, v in ExternalDailySnapshot.__cache:
            if k == key:
                return v
        val = cls.__latest(date, lookback, args, kwargs)
        ExternalDailySnapshot.__cache.append((key, val))
        return val

    @classmethod
    def __latest(cls, date, lookback, args, kwargs):
        assert lookback > 0
        t = None
        for i in range(lookback):
            d = date - datetime.timedelta(i)
            t = cls(date=d, *args, **kwargs)
            if t.complete():
                return t
        logger.debug("Could not find last dump for %s (looked back %d days)",
                     cls.__name__, lookback)
        return t
