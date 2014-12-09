import luigi
import luigi.contrib.mpi as mpi
import time
import random
import os
from mpi4py import MPI

COM = MPI.COMM_WORLD

TEMPDIR = '/tmp/work'

class DummyTask(luigi.Task):
    id = luigi.Parameter()

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(TEMPDIR, str(self.id)))


class SleepyTask(luigi.Task):
    id = luigi.Parameter()

    def run(self):
        time.sleep(random.uniform(0,2))
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(TEMPDIR, str(self.id)))


class ChainedSleepyTask(SleepyTask):
    id = luigi.Parameter()

    def requires(self):
        if int(self.id) > 0:
            return [ChainedSleepyTask(int(self.id)-1)]
        else:
            return []


def test0():
    print 'test0', '%i/%i' % (COM.Get_rank(), COM.Get_size())


def test1():
    tasks = [DummyTask(id) for id in range(20)]
    mpi.run(tasks)
    
    if COM.Get_rank() == 0:
        for t in tasks:
            assert (t.complete() is True)


def test2():
    tasks = [SleepyTask(id) for id in range(20, 30)]
    mpi.run(tasks)
    
    if COM.Get_rank() == 0:
        for t in tasks:
            assert (t.complete() is True)


def test3():
    tasks = [ChainedSleepyTask(35)]
    mpi.run(tasks)
    
    if COM.Get_rank() == 0:
        for t in tasks:
            assert (t.complete() is True)

def test4():
    tasks = [DummyTask(id) for id in range(20)]

    if COM.Get_rank() == 0:
        config = luigi.configuration.get_config()
        if not config.has_option('task_history', 'db_connection'):
            config.add_section('task_history')
            config.set('task_history', 'db_connection', 'sqlite:///history.db')
        filename = config.get('task_history', 'db_connection').split('///')[-1]
        from luigi.db_task_history import DbTaskHistory
        mpi.run(tasks, task_history=DbTaskHistory())
        assert os.path.exists(filename)
        os.remove(filename)
    else:
        mpi.run(tasks)

if __name__ == '__main__':
    test0()
    COM.Barrier()

    test1()
    COM.Barrier()

    test2()
    COM.Barrier()

    test3()
    COM.Barrier()

    test4()
    COM.Barrier()

    # if COM.Get_rank() == 0:
    #     from pprint import pprint
    #     pprint(sch.scheduler.graph())