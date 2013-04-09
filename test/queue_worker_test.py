import luigi
import luigi.hdfs
import luigi.queue_worker
import tempfile
import unittest


class TaskWithArgs(luigi.Task):
    string_arg = luigi.Parameter()


class ArgParseTaskDeserializerTest(unittest.TestCase):
    def test_task_with_args(self):
        deserializer = luigi.queue_worker.ArgParseTaskDeserializer()
        task = deserializer.deserialize('TaskWithArgs', ['--string-arg', 'foo'])
        self.assertEquals(task, TaskWithArgs(string_arg='foo'))

    def test_task_missing_args(self):
        deserializer = luigi.queue_worker.ArgParseTaskDeserializer()
        should_raise = lambda: deserializer.deserialize('TaskWithArgs', [])
        self.assertRaises(luigi.queue_worker.DeserializerException, should_raise)
