from helpers import LuigiTestCase


import luigi
import luigi.worker
import luigi.execution_summary


class DummyRequires(luigi.Task):
    def run(self):
        print('just a dummy task')


class FailInRun(luigi.Task):
    def run(self):
        print('failing in run')
        raise Exception


class FailInRequires(luigi.Task):
    def requires(self):
        print('failing')
        raise Exception

    def run(self):
        print('running')


class FailInDepRequires(luigi.Task):
    def requires(self):
        return [FailInRequires()]

    def run(self):
        print('doing a thing')


class FailInDepRun(luigi.Task):
    def requires(self):
        return [FailInRun()]

    def run(self):
        print('doing a thing')


class UnknownStateTest(LuigiTestCase):
    def setUp(self):
        super(UnknownStateTest, self).setUp()
        self.scheduler = luigi.scheduler.Scheduler(
            prune_on_get_work=False,
            retry_count=1
        )
        self.worker = luigi.worker.Worker(
            scheduler=self.scheduler, 
            keep_alive=True
        )

    def run_task(self, task):
        self.worker.add(task)  # schedule
        self.worker.run()  # run

    def summary_dict(self):
        return luigi.execution_summary._summary_dict(self.worker)

    def test_fail_in_run(self):
        self.run_task(FailInRun())
        summary_dict = self.summary_dict()

        self.assertEqual({FailInRun()}, summary_dict['failed'])

    def test_fail_in_requires(self):
        self.run_task(FailInRequires())
        summary_dict = self.summary_dict()

        self.assertEqual({FailInRequires()}, summary_dict['scheduling_error'])

    def test_fail_in_dep_run(self):
        self.run_task(FailInDepRun())
        summary_dict = self.summary_dict()

        self.assertEqual({FailInRun()}, summary_dict['failed'])
        self.assertEqual({FailInDepRun()}, summary_dict['still_pending_not_ext'])

    def test_fail_in_dep_requires(self):
        self.run_task(FailInDepRequires())
        summary_dict = self.summary_dict()

        self.assertEqual({FailInRequires()}, summary_dict['scheduling_error'])
        self.assertEqual({FailInDepRequires()}, summary_dict['still_pending_not_ext'])
