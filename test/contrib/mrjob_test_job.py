# -*- coding: utf-8 -*-
#
# Copyright 2016 Enreach Oy
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
A really simple MRJob job that can be used in testing.

The if __name__ == "__main__" is really important for MRJob automation so this
was separated into it's own file. It could probably be merged into the test
file but that would introduce some difficult to understand magic into why the
local runner test works.

"""

import mrjob.examples.mr_word_freq_count
import json


class MRFreqWithCounters(mrjob.examples.mr_word_freq_count.MRWordFreqCount):
    def reducer(self, word, counts):
        sum_ = sum(counts)
        # increment a counter so we can test counter parsing functionality
        self.increment_counter("test_counters", "words_seen_total", sum_)
        if self.options.invert_words:
            yield word[::-1], sum_
        elif self.options.lookup_file:
            with open(self.options.lookup_file, "r") as instr:
                lookup = json.load(instr)
                yield lookup[word], sum_
        else:
            yield word, sum_

    def configure_options(self):
        super(MRFreqWithCounters, self).configure_options()
        self.add_passthrough_option("--invert-words", action="store_true",
                                    dest="invert_words", default=False)
        self.add_file_option('--lookup-file', dest="lookup_file")


if __name__ == "__main__":
    MRFreqWithCounters.run()
