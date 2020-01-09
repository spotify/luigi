import abc
import collections
import itertools
import logging
import os
import time

try:
    import cPickle as pickle
except ImportError:
    import pickle

from luigi import six
from luigi import notifications
from luigi.scheduler import Worker
from luigi.task_status import DISABLED, DONE, FAILED, PENDING, RUNNING, BATCH_RUNNING

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class SchedulerState(object):

    # -------------------------------------------------------------------------------------------------------

    @abc.abstractmethod
    def dump(self):
        """
        Dumps the state into some sort of storage, config stored in `self`
        """
        pass

    @abc.abstractmethod
    def load(self):
        """
        Loads the state from some kind of storage, config stored in `self`
        """
        pass

    @abc.abstractmethod
    def get_active_tasks(self):
        """
        Gets a list of `scheduler.Task` obejcts, including all statuses
        """
        pass

    @abc.abstractmethod
    def get_active_tasks_by_status(self, *statuses):
        """
        Gets a list of `scheduler.Task` objects, filtered to certain statuses
        """
        pass

    @abc.abstractmethod
    def set_batcher(self, worker_id, family, batcher_args, max_batch_size):
        """
        """
        pass

    @abc.abstractmethod
    def get_batcher(self, worker_id, family):
        """
        """
        pass

    @abc.abstractmethod
    def get_task(self, task_id, default=None, setdefault=None):
        """
        Gets the task whose ID is == `task_id`
        Given `setdefault`, it will create the task when it isn't found in the store
        """
        pass

    @abc.abstractmethod
    def persist_task(self, task):
        """
        Persists the entire task to the store, under the key `task.id`
        """
        pass

    @abc.abstractmethod
    def inactivate_tasks(self, delete_tasks):
        """
        The terminology is a bit confusing: we used to "delete" tasks when they became inactive,
        but with a pluggable state storage, you might very well want to keep some history of
        older tasks as well. That's why we call it "inactivate" (as in the verb)
        """
        pass

    @abc.abstractmethod
    def get_active_workers(self, last_active_lt=None, last_get_work_gt=None):
        """
        Gets a list of all active workers, filtered by `last_active_lt` and `last_get_work_gt`
        (if they are supplied)
        """
        pass

    @abc.abstractmethod
    def get_worker(self, worker_id):
        """
        Gets a `scheduler.Worker` object based on the ID of the worker
        """
        pass

    @abc.abstractmethod
    def inactivate_workers(self, delete_workers):
        """
        Sets a worker to inactive (see definition of "inactivate" above)
        """
        pass

    @abc.abstractmethod
    def update_metrics(self, task, config):
        """
        """
        pass

    # -------------------------------------------------------------------------------------------------------

    def get_active_task_count_for_status(self, status):
        """
        Gets the number of tasks from the central scheduler that have status `status`
        """
        if status:
            return sum(1 for x in self.get_active_tasks_by_status(status))
        else:
            return sum(1 for x in self.get_active_tasks())

    def get_batch_running_tasks(self, batch_id):
        """
        Gets all batch-running tasks that have batch ID `batch_id`
        """
        return [
            task for task in self.get_active_tasks_by_status(BATCH_RUNNING)
            if task.batch_id == batch_id
        ]

    def num_pending_tasks(self):
        """
        Gets the total number of tasks that are either in a PENDING or RUNNING state
        """
        return sum(1 for x in self.get_active_tasks_by_status(PENDING, RUNNING))

    def set_batch_running(self, task, batch_id, worker_id):
        """
        """
        task.batch_id = batch_id
        task.worker_running = worker_id
        task.resources_running = task.resources
        task.time_running = time.time()
        self.set_status(task, BATCH_RUNNING)

    def has_task(self, task_id):
        """
        """
        return self.get_task(task_id) is not None

    def set_status(self, task, new_status, config=None):
        """
        """
        if new_status == FAILED:
            assert config is not None

        if new_status == DISABLED and task.status in (RUNNING, BATCH_RUNNING):
            return

        remove_on_failure = task.batch_id is not None and not task.batchable

        if task.status == DISABLED:
            if new_status == DONE:
                self.re_enable(task)

            # don't allow workers to override a scheduler disable
            elif task.scheduler_disable_time is not None and new_status != DISABLED:
                return

        if task.status == RUNNING and task.batch_id is not None and new_status != RUNNING:
            for batch_task in self.get_batch_running_tasks(task.batch_id):
                self.set_status(batch_task, new_status, config)
                batch_task.batch_id = None
            task.batch_id = None

        if new_status == FAILED and task.status != DISABLED:
            task.add_failure()
            if task.has_excessive_failures():
                task.scheduler_disable_time = time.time()
                new_status = DISABLED
                if not config.batch_emails:
                    notifications.send_error_email(
                        'Luigi Scheduler: DISABLED {task} due to excessive failures'.format(task=task.id),
                        '{task} failed {failures} times in the last {window} seconds, so it is being '
                        'disabled for {persist} seconds'.format(
                            failures=task.retry_policy.retry_count,
                            task=task.id,
                            window=config.disable_window,
                            persist=config.disable_persist,
                        ))
        elif new_status == DISABLED:
            task.scheduler_disable_time = None

        if new_status == FAILED:
            task.retry = time.time() + config.retry_delay
            if remove_on_failure:
                task.remove = time.time()

        if new_status != task.status:
            task.status = new_status
            task.updated = time.time()
            self.update_metrics(task, config)

        self.persist_task(task)

    def re_enable(self, task, config=None):
        """
        """
        task.scheduler_disable_time = None
        task.failures.clear()
        if config:
            self.set_status(task, FAILED, config)
            task.failures.clear()

    def may_prune(self, task):
        """
        """
        return task.remove and time.time() >= task.remove

    def fail_dead_worker_task(self, task, config, assistants):
        """
        If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
        """
        if task.status in (BATCH_RUNNING, RUNNING) and \
           task.worker_running and \
           task.worker_running not in task.stakeholders | assistants:
            logger.info("Task %r is marked as running by disconnected worker %r -> marking as "
                        "FAILED with retry delay of %rs", task.id, task.worker_running,
                        config.retry_delay)
            task.worker_running = None
            self.set_status(task, FAILED, config)
            task.retry = time.time() + config.retry_delay

    def update_status(self, task, config):
        # Mark tasks with no remaining active stakeholders for deletion
        if (not task.stakeholders) and (task.remove is None) and (task.status != RUNNING):
            # We don't check for the RUNNING case, because that is already handled
            # by the fail_dead_worker_task function.
            logger.debug("Task %r has no stakeholders anymore -> might remove "
                         "task in %s seconds", task.id, config.remove_delay)
            task.remove = time.time() + config.remove_delay

        # Re-enable task after the disable time expires
        if task.status == DISABLED and task.scheduler_disable_time is not None:
            if time.time() - task.scheduler_disable_time > config.disable_persist:
                self.re_enable(task, config)

        # Reset FAILED tasks to PENDING if max timeout is reached, and retry delay is >= 0
        if task.status == FAILED and config.retry_delay >= 0 and task.retry < time.time():
            self.set_status(task, PENDING, config)

    def get_assistants(self, last_active_lt=None):
        return filter(lambda w: w.assistant, self.get_active_workers(last_active_lt))

    def get_worker_ids(self):
        return [worker.id for worker in self.get_active_workers()]

    def _remove_workers_from_tasks(self, workers, remove_stakeholders=True):
        for task in self.get_active_tasks():
            if remove_stakeholders:
                task.stakeholders.difference_update(workers)
            task.workers -= workers
            self.persist_task(task)

    def disable_workers(self, worker_ids):
        self._remove_workers_from_tasks(worker_ids, remove_stakeholders=False)
        for worker_id in worker_ids:
            worker = self.get_worker(worker_id)
            worker.disabled = True
            worker.tasks.clear()


class DBTask(Base):
    """
    Class representing a single Luigi task stored as a row in a database
    """
    __tablename__ = 'luigi_task_state'

    task_id = Column(String(255), primary_key=True)
    status = Column(String(100))
    pickled = Column(String(10000))


class SqlSchedulerState(SchedulerState):
    """
    Keep track of the current state and handle persistance backed by a SQL task table.
    """
    def __init__(self, mysql_target):

        self.engine = create_engine(mysql_target)
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)

        # TODO 2020-01-07 make task batchers persisted to DB as well
        self._task_batchers = {}

        # TODO 2020-01-07 make active workers persisted to DB as well
        self._active_workers = {}

        self._metrics_collector = None

    def dump(self):
        pass  # always persisted

    def load(self):
        pass  # always persisted

    def _try_unpickle(self, db_task):
        try:
            return pickle.loads(db_task.pickled)
        except (pickle.UnpicklingError,EOFError) as e:
            logger.warning("Warning, unable to de-pickle task {}".format(db_task.task_id))
            return None

    def get_active_tasks(self):
        session = self.session()
        db_res = session.query(DBTask).all()
        session.close()
        return itertools.ifilter(lambda t: t, (self._try_unpickle(t) for t in db_res))

    def get_active_tasks_by_status(self, *statuses):
        session = self.session()
        db_res = session.query(DBTask).filter(DBTask.status.in_(statuses)).all()
        session.close()
        return itertools.ifilter(lambda t: t, (self._try_unpickle(t) for t in db_res))

    def set_batcher(self, worker_id, family, batcher_args, max_batch_size):
        self._task_batchers.setdefault(worker_id, {})
        self._task_batchers[worker_id][family] = (batcher_args, max_batch_size)

    def get_batcher(self, worker_id, family):
        return self._task_batchers.get(worker_id, {}).get(family, (None, 1))

    def get_task(self, task_id, default=None, setdefault=None):
        session = self.session()
        db_task = session.query(DBTask).filter(DBTask.task_id == task_id).first()
        session.close()
        if db_task:
            res = self._try_unpickle(db_task)
        elif setdefault:
            res = self.persist_task(setdefault)
        else:
            res = default
        return res

    def persist_task(self, task):
        session = self.session()
        db_task = session.query(DBTask).filter(DBTask.task_id == task.id).first()
        if db_task:
            db_task.status = task.status
            db_task.pickled = pickle.dumps(task, protocol=2)
        else:
            new_task = DBTask(
                task_id=task.id,
                status=task.status,
                pickled=pickle.dumps(task, protocol=2)
            )
            session.add(new_task)
        session.commit()
        session.close()
        return task

    def inactivate_tasks(self, delete_tasks):
        for task in delete_tasks:
            session = self.session()
            db_task = session.query(DBTask).filter(DBTask.task_id == task.id).first()
            if db_task:
                session.delete(db_task)
                session.commit()
            else:
                logger.warn("Tried to inactivate task that doesn't exist: {}".format(task))
            session.close()

    def get_active_workers(self, last_active_lt=None, last_get_work_gt=None):
        for worker in six.itervalues(self._active_workers):
            if last_active_lt is not None and worker.last_active >= last_active_lt:
                continue
            last_get_work = worker.last_get_work
            if last_get_work_gt is not None and (
                            last_get_work is None or last_get_work <= last_get_work_gt):
                continue
            yield worker

    def get_worker(self, worker_id):
        return self._active_workers.setdefault(worker_id, Worker(worker_id))

    def inactivate_workers(self, delete_workers):
        for worker in delete_workers:
            self._active_workers.pop(worker)
        self._remove_workers_from_tasks(delete_workers)

    def update_metrics(self, task, config):
        if task.status == DISABLED:
            self._metrics_collector.handle_task_disabled(task, config)
        elif task.status == DONE:
            self._metrics_collector.handle_task_done(task)
        elif task.status == FAILED:
            self._metrics_collector.handle_task_failed(task)


class SimpleSchedulerState(SchedulerState):
    """
    Keep track of the current state and handle persistence using an in-memory dictionary.
    """

    def __init__(self, state_path):
        self._state_path = state_path
        self._tasks = {}  # map from id to a Task object
        self._status_tasks = collections.defaultdict(dict)
        self._active_workers = {}  # map from id to a Worker object
        self._task_batchers = {}
        self._metrics_collector = None

    def get_state(self):
        return self._tasks, self._active_workers, self._task_batchers

    def set_state(self, state):
        self._tasks, self._active_workers = state[:2]
        if len(state) >= 3:
            self._task_batchers = state[2]

    def dump(self):
        try:
            with open(self._state_path, 'wb') as fobj:
                pickle.dump(self.get_state(), fobj)
        except IOError:
            logger.warning("Failed saving scheduler state", exc_info=1)
        else:
            logger.info("Saved state in %s", self._state_path)

    # prone to lead to crashes when old state is unpickled with updated code. TODO some kind of version control?
    def load(self):
        if os.path.exists(self._state_path):
            logger.info("Attempting to load state from %s", self._state_path)
            try:
                with open(self._state_path, 'rb') as fobj:
                    state = pickle.load(fobj)
            except BaseException:
                logger.exception("Error when loading state. Starting from empty state.")
                return

            self.set_state(state)
            self._status_tasks = collections.defaultdict(dict)
            for task in six.itervalues(self._tasks):
                self._status_tasks[task.status][task.id] = task
        else:
            logger.info("No prior state file exists at %s. Starting with empty state", self._state_path)

    def get_active_tasks(self):
        return six.itervalues(self._tasks)

    def get_active_tasks_by_status(self, *statuses):
        return itertools.chain.from_iterable(six.itervalues(self._status_tasks[status]) for status in statuses)

    def set_batcher(self, worker_id, family, batcher_args, max_batch_size):
        self._task_batchers.setdefault(worker_id, {})
        self._task_batchers[worker_id][family] = (batcher_args, max_batch_size)

    def get_batcher(self, worker_id, family):
        return self._task_batchers.get(worker_id, {}).get(family, (None, 1))

    def get_task(self, task_id, default=None, setdefault=None):
        if setdefault:
            task = self._tasks.setdefault(task_id, setdefault)
            self._status_tasks[task.status][task.id] = task
            return task
        else:
            return self._tasks.get(task_id, default)

    def persist_task(self, task):
        # remove the task from old status dict if it now has a new status
        for status, task_dict in self._status_tasks.items():
            if task.id in task_dict.keys():
                task_dict.pop(task.id)
        self._tasks[task.id] = task
        self._status_tasks[task.status][task.id] = task

    def inactivate_tasks(self, delete_tasks):
        for task_id in delete_tasks:
            task_obj = self._tasks.pop(task_id)
            self._status_tasks[task_obj.status].pop(task_id)

    def get_active_workers(self, last_active_lt=None, last_get_work_gt=None):
        for worker in six.itervalues(self._active_workers):
            if last_active_lt is not None and worker.last_active >= last_active_lt:
                continue
            last_get_work = worker.last_get_work
            if last_get_work_gt is not None and (
                            last_get_work is None or last_get_work <= last_get_work_gt):
                continue
            yield worker

    def get_worker(self, worker_id):
        return self._active_workers.setdefault(worker_id, Worker(worker_id))

    def inactivate_workers(self, delete_workers):
        for worker in delete_workers:
            self._active_workers.pop(worker)
        self._remove_workers_from_tasks(delete_workers)

    def update_metrics(self, task, config):
        if task.status == DISABLED:
            self._metrics_collector.handle_task_disabled(task, config)
        elif task.status == DONE:
            self._metrics_collector.handle_task_done(task)
        elif task.status == FAILED:
            self._metrics_collector.handle_task_failed(task)
