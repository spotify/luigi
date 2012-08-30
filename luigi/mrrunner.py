#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2008 Spotify AB

"""The hadoop runner.

This module contains the main() method which will be used to run the
mapper and reducer on the Hadoop nodes.
"""

import os
import sys
import tarfile
import cPickle as pickle
import logging
import traceback

# these methods needs to be available before the rest of spotify.rambo is unpacked
def repr_reader(inputs):
    """Reader which uses python eval on each part of a tab separated string.
       Yields a tuple of python objects."""
    for input in inputs:
        yield map(eval, input.split("\t"))


def repr_writer(outputs):
    """Writer which outpus the python repr for each item"""
    for output in outputs:
        print "\t".join(map(repr, output))


class Runner(object):
    """Run the mapper or reducer on hadoop nodes."""

    def __init__(self, job=None):
        self.extract_packages_archive()
        self.job = job or pickle.load(open("job-instance.pickle"))

    def run(self, kind):
        try:
            if kind == "map":
                self.job._run_mapper()
            if kind == "combiner":
                self.job._run_combiner()
            elif kind == "reduce":
                self.job._run_reducer()
        except:
            # Dump encoded data that we will try to fetch using mechanize
            exc = traceback.format_exc()
            self.job._print_exception(exc)
            raise

    def extract_packages_archive(self):
        if not os.path.exists("packages.tar"):
            return

        tar = tarfile.open("packages.tar")
        for tarinfo in tar:
            tar.extract(tarinfo)
        tar.close()
        if '' not in sys.path:
            sys.path.insert(0, '')


def main():
    """Run either the mapper or the reducer from the class instance in the file "job-instance.pickle".

    Arguments:

    kind -- is either map or reduce
    """
    # Set up logging.
    logging.basicConfig(level=logging.WARN)

    kind = sys.argv[1]
    Runner().run(kind)

if __name__ == '__main__':
    main()
