# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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

import datetime
import re


class DateInterval(object):

    def __init__(self, date_a, date_b):
        # Represents all date d such that date_a <= d < date_b
        self.date_a = date_a
        self.date_b = date_b

    def dates(self):
        dates = []
        d = self.date_a
        while d < self.date_b:
            dates.append(d)
            d += datetime.timedelta(1)

        return dates

    def hours(self):
        for date in self.dates():
            for hour in xrange(24):
                yield datetime.datetime.combine(date, datetime.time(hour))

    def __str__(self):
        return self.to_string()

    def __repr__(self):
        return self.to_string()

    def prev(self):
        return self.from_date(self.date_a - datetime.timedelta(1))

    def next(self):
        return self.from_date(self.date_b)

    def to_string(self):
        raise NotImplementedError

    @classmethod
    def from_date(cls, d):
        raise NotImplementedError

    @classmethod
    def parse(cls, s):
        raise NotImplementedError

    def __contains__(self, date):
        return date in self.dates()

    def __iter__(self):
        for d in self.dates():
            yield d

    def __hash__(self):
        return hash(repr(self))

    def __cmp__(self, other):
        if not isinstance(self, type(other)):
            # doing this because it's not well defined if eg. 2012-01-01-2013-01-01 == 2012
            raise TypeError('Date interval type mismatch')
        return cmp((self.date_a, self.date_b), (other.date_a, other.date_b))

    def __eq__(self, other):
        if not isinstance(other, DateInterval):
            return False
        else:
            return self.__cmp__(other) == 0

    def __ne__(self, other):
        return not self.__eq__(other)


class Date(DateInterval):

    def __init__(self, y, m, d):
        a = datetime.date(y, m, d)
        b = datetime.date(y, m, d) + datetime.timedelta(1)
        super(Date, self).__init__(a, b)

    def to_string(self):
        return self.date_a.strftime('%Y-%m-%d')

    @classmethod
    def from_date(cls, d):
        return Date(d.year, d.month, d.day)

    @classmethod
    def parse(cls, s):
        if re.match(r'\d\d\d\d\-\d\d\-\d\d$', s):
            return Date(*map(int, s.split('-')))


class Week(DateInterval):

    def __init__(self, y, w):
        # Python datetime does not have a method to convert from ISO weeks!
        for d in xrange(-10, 370):
            date = datetime.date(y, 1, 1) + datetime.timedelta(d)
            if date.isocalendar() == (y, w, 1):
                date_a = date
                break
        else:
            raise ValueError('Invalid week')
        date_b = date_a + datetime.timedelta(7)
        super(Week, self).__init__(date_a, date_b)

    def to_string(self):
        return '%d-W%02d' % self.date_a.isocalendar()[:2]

    @classmethod
    def from_date(cls, d):
        return Week(*d.isocalendar()[:2])

    @classmethod
    def parse(cls, s):
        if re.match(r'\d\d\d\d\-W\d\d$', s):
            y, w = map(int, s.split('-W'))
            return Week(y, w)


class Month(DateInterval):

    def __init__(self, y, m):
        date_a = datetime.date(y, m, 1)
        date_b = datetime.date(y + m / 12, 1 + m % 12, 1)
        super(Month, self).__init__(date_a, date_b)

    def to_string(self):
        return self.date_a.strftime('%Y-%m')

    @classmethod
    def from_date(cls, d):
        return Month(d.year, d.month)

    @classmethod
    def parse(cls, s):
        if re.match(r'\d\d\d\d\-\d\d$', s):
            y, m = map(int, s.split('-'))
            return Month(y, m)


class Year(DateInterval):

    def __init__(self, y):
        date_a = datetime.date(y, 1, 1)
        date_b = datetime.date(y + 1, 1, 1)
        super(Year, self).__init__(date_a, date_b)

    def to_string(self):
        return self.date_a.strftime('%Y')

    @classmethod
    def from_date(cls, d):
        return Year(d.year)

    @classmethod
    def parse(cls, s):
        if re.match(r'\d\d\d\d$', s):
            return Year(int(s))


class Custom(DateInterval):

    def to_string(self):
        return '-'.join([d.strftime('%Y-%m-%d') for d in (self.date_a, self.date_b)])

    @classmethod
    def parse(cls, s):
        if re.match('\d\d\d\d\-\d\d\-\d\d\-\d\d\d\d\-\d\d\-\d\d$', s):
            # Actually the ISO 8601 specifies <start>/<end> as the time interval format
            # Not sure if this goes for date intervals as well. In any case slashes will
            # most likely cause problems with paths etc.
            x = map(int, s.split('-'))
            date_a = datetime.date(*x[:3])
            date_b = datetime.date(*x[3:])
            return Custom(date_a, date_b)
