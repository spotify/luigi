from unittest import TestCase
from luigi import Task, build, Event
import luigi

class DummyException(Exception):
    pass

class EmptyTask(Task):
    fail = luigi.BooleanParameter()

    def run(self):
        if self.fail:
            raise DummyException()


class TaskWithCallback(Task):
    def run(self):
        print "Triggering event"
        self.trigger_event("foo event")


class TestEventCallbacks(TestCase):
    def test_success_handler(self):
        saved_tasks = []

        @EmptyTask.event_handler(Event.SUCCESS)
        def save_task(task):
            print "Saving task..."
            saved_tasks.append(task)
 
        t = EmptyTask(False)
        build([t], local_scheduler=True)
        self.assertEquals(saved_tasks[0], t)

    def test_failure_handler(self):
        exceptions = []

        @EmptyTask.event_handler(Event.FAILURE)
        def save_task(task, exception):
            print "Saving exception..."
            exceptions.append(exception)
 
        t = EmptyTask(True)
        build([t], local_scheduler=True)
        self.assertEquals(type(exceptions[0]), DummyException)

    def test_custom_handler(self):
        dummies = []

        @TaskWithCallback.event_handler("foo event")
        def story_dummy():
            dummies.append("foo")

        t = TaskWithCallback()
        build([t], local_scheduler=True)
        self.assertEquals(dummies[0], "foo")
