'''
Requires HTCondor(https://research.cs.wisc.edu/htcondor) to be
installed & configured on the machine.

You can run this example like this:

    .. code:: console

            $ luigi --module examples.hello_htcondor examples.HelloHTCondorTask --local-scheduler

If that does not work, see :ref:`CommandLine`.
'''
import luigi.contrib.htcondor


class HelloHTCondorTask(luigi.contrib.htcondor.HTCondorJobTask):
    task_namespace = 'examples'

    def work(self):
        print("{task} says: Hello world!".format(task=self.__class__.__name__))


if __name__ == '__main__':
    luigi.run(
        ['examples.HelloHTCondorTask', '--workers', '1', '--local-scheduler'])
