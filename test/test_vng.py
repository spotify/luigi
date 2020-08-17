import time
from threading import Thread
import datetime as dt

import luigi
from luigi import ExternalTask, LocalTarget, Task, WrapperTask

class ExternalDummy2(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget('/home/baopng/Downloads/tmp/f0_component')


class MyTask(luigi.Task):
    def requires(self):
        yield ExternalDummy2()

    def output(self):
        return luigi.LocalTarget('/home/baopng/Downloads/tmp/luigi_test4')

    def run(self):
        res = []
        for input in self.input():
            with input.open('r') as in_file:
                count = 0
                for _ in in_file:
                    count += 1
                res.append(str(count))
        with self.output().open('w') as out_file:
            out_file.write(' '.join(res))


class DummyLog(ExternalTask):
    folder = '/home/baopng/Downloads/tmp/f0_component'

    # time_params = None

    def output(self):
        return LocalTarget(self.folder)


class DummyTask(Task):
    folder = '/home/baopng/Downloads/tmp/luigi_test26'
    #
    # resources = {'a': 1}

    schedule = (dt.time(23, 34), dt.time(23, 50))

    def requires(self):
        yield DummyLog()

    def output(self):
        return LocalTarget(self.folder)

    # def process_resources(self):
    #     # if dt.datetime.now().minute >= 15:
    #     #     self.resources['time'] = 1
    #     return self.resources

    def complete(self):
        # if dt.datetime.now().minute >= 52:
        #     self.resources['time'] = 1
        return super().complete()

    def run(self):
        res = []
        for input in self.input():
            with input.open('r') as in_file:
                count = 0
                for _ in in_file:
                    count += 1
                res.append(str(count))
        with self.output().open('w') as out_file:
            out_file.write(' '.join(res))


class TestWrapper(WrapperTask):

    # time_params = None

    def requires(self):
        yield DummyTask()


if __name__ == '__main__':
    luigi.build([TestWrapper()], local_scheduler=False)
    # a = DummyTask()
    # print(a.resources)
    # time.sleep(10)
    # print(a.resources)
