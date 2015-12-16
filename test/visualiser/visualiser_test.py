"""
Test the visualiser's javascript using PhantomJS.

"""

from __future__ import print_function

import os
import luigi
import subprocess
import sys
import unittest
import time
import threading

here = os.path.dirname(__file__)

# Patch-up path so that we can import from the directory above this one.
# This seems to be necessary because the `test` directory has no __init__.py but
# adding one makes other tests fail.
sys.path.append(os.path.join(here, '..'))
from server_test import ServerTestBase

TEST_TIMEOUT = 40


@unittest.skipUnless(os.environ.get('TEST_VISUALISER'),
                     'PhantomJS tests not requested in TEST_VISUALISER')
class TestVisualiser(ServerTestBase):
    """
    Builds a medium-sized task tree of MergeSort results then starts
    phantomjs  as a subprocess to interact with the scheduler.

    """

    def setUp(self):
        super(TestVisualiser, self).setUp()

        x = 'I scream for ice cream'
        task = UberTask(base_task=FailingMergeSort, x=x, copies=4)
        luigi.build([task], workers=1, scheduler_port=self.get_http_port())

        self.done = threading.Event()

        def _do_ioloop():
            # Enter ioloop for maximum TEST_TIMEOUT.  Check every 2s whether the test has finished.
            print('Entering event loop in separate thread')

            for i in range(TEST_TIMEOUT):
                try:
                    self.wait(timeout=1)
                except AssertionError:
                    pass
                if self.done.is_set():
                    break

            print('Exiting event loop thread')

        self.iothread = threading.Thread(target=_do_ioloop)
        self.iothread.start()

    def tearDown(self):
        self.done.set()
        self.iothread.join()

    def test(self):
        port = self.get_http_port()
        print('Server port is {}'.format(port))
        print('Starting phantomjs')

        p = subprocess.Popen('phantomjs {}/phantomjs_test.js http://localhost:{}'.format(here, port),
                             shell=True, stdin=None)

        # PhantomJS may hang on an error so poll
        status = None
        for x in range(TEST_TIMEOUT):
            status = p.poll()
            if status is not None:
                break
            time.sleep(1)

        if status is None:
            raise AssertionError('PhantomJS failed to complete')
        else:
            print('PhantomJS return status is {}'.format(status))
            assert status == 0


# ---------------------------------------------------------------------------
# Code for generating a tree of tasks with some failures.

def generate_task_families(task_class, n):
    """
    Generate n copies of a task with different task_family names.

    :param task_class: a subclass of `luigi.Task`
    :param n: number of copies of `task_class` to create
    :return: Dictionary of task_family => task_class

    """
    ret = {}
    for i in range(n):
        class_name = '{}_{}'.format(task_class.task_family, i)
        ret[class_name] = type(class_name, (task_class,), {})

    return ret


class UberTask(luigi.Task):
    """
    A task which depends on n copies of a configurable subclass.

    """
    _done = False

    base_task = luigi.Parameter()
    x = luigi.Parameter()
    copies = luigi.IntParameter()

    def requires(self):
        task_families = generate_task_families(self.base_task, self.copies)
        for class_name in task_families:
            yield task_families[class_name](x=self.x)

    def complete(self):
        return self._done

    def run(self):
        self._done = True


def popmin(a, b):
    """
    popmin(a, b) -> (i, a', b')

    where i is min(a[0], b[0]) and a'/b' are the results of removing i from the
    relevant sequence.
    """
    if len(a) == 0:
        return b[0], a, b[1:]
    elif len(b) == 0:
        return a[0], a[1:], b
    elif a[0] > b[0]:
        return b[0], a, b[1:]
    else:
        return a[0], a[1:], b


class MemoryTarget(luigi.Target):
    def __init__(self):
        self.box = None

    def exists(self):
        return self.box is not None


class MergeSort(luigi.Task):
    x = luigi.Parameter(description='A string to be sorted')

    def __init__(self, *args, **kwargs):
        super(MergeSort, self).__init__(*args, **kwargs)

        self.result = MemoryTarget()

    def requires(self):
        # Allows us to override behaviour in subclasses
        cls = self.__class__

        if len(self.x) > 1:
            p = len(self.x) // 2

            return [cls(self.x[:p]), cls(self.x[p:])]

    def output(self):
        return self.result

    def run(self):
        if len(self.x) > 1:
            list_1, list_2 = (x.box for x in self.input())

            s = []
            while list_1 or list_2:
                item, list_1, list_2 = popmin(list_1, list_2)
                s.append(item)
        else:
            s = self.x

        self.result.box = ''.join(s)


class FailingMergeSort(MergeSort):
    """
    Simply fail if the string to sort starts with ' '.

    """
    fail_probability = luigi.FloatParameter(default=0.)

    def run(self):
        if self.x[0] == ' ':
            raise Exception('I failed')
        else:
            return super(FailingMergeSort, self).run()


if __name__ == '__main__':
    x = 'I scream for ice cream'
    task = UberTask(base_task=FailingMergeSort, x=x, copies=4)
    luigi.build([task], workers=1, scheduler_port=8082)
