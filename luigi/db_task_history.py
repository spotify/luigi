# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import task_history
import configuration
import datetime
import logging

from contextlib import contextmanager
from task_status import PENDING, FAILED, DONE, RUNNING

from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

logger = logging.getLogger('luigi-interface')


class DbTaskHistory(task_history.TaskHistory):
    """ Task History that writes to a database using sqlalchemy. Also has methods for useful db queries
    """
    @contextmanager
    def _session(self, session=None):
        if session:
            yield session
        else:
            session = self.session_factory()
            try:
                yield session
            except:
                session.rollback()
                raise
            else:
                session.commit()

    def __init__(self):
        config = configuration.get_config()
        connection_string = config.get('task_history', 'db_connection')
        self.engine = create_engine(connection_string)
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)
        self.tasks = {}  # task_id -> TaskRecord

    def task_scheduled(self, task_id):
        task = self._get_task(task_id, status=PENDING)
        self._add_task_event(task, TaskEvent(event_name=PENDING, ts=datetime.datetime.now()))

    def task_finished(self, task_id, successful):
        event_name = DONE if successful else FAILED
        task = self._get_task(task_id, status=event_name)
        self._add_task_event(task, TaskEvent(event_name=event_name, ts=datetime.datetime.now()))

    def task_started(self, task_id, worker_host):
        task = self._get_task(task_id, status=RUNNING, host=worker_host)
        self._add_task_event(task, TaskEvent(event_name=RUNNING, ts=datetime.datetime.now()))

    def _get_task(self, task_id, status, host=None):
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.status = status
            if host:
                task.host = host
        else:
            task = self.tasks[task_id] = task_history.Task(task_id, status, host)
        return task

    def _add_task_event(self, task, event):
        for (task_record, session) in self._find_or_create_task(task):
            task_record.events.append(event)

    def _find_or_create_task(self, task):
        with self._session() as session:
            if task.record_id is not None:
                logger.debug("Finding task with record_id [%d]", task.record_id)
                task_record = session.query(TaskRecord).get(task.record_id)
                if not task_record:
                    raise Exception("Task with record_id, but no matching Task record!")
                yield (task_record, session)
            else:
                task_record = TaskRecord(name=task.task_family, host=task.host)
                for (k, v) in task.parameters.iteritems():
                    task_record.parameters[k] = TaskParameter(name=k, value=v)
                session.add(task_record)
                yield (task_record, session)
            if task.host:
                task_record.host = task.host
        task.record_id = task_record.id

    def find_all_by_parameters(self, task_name, session=None, **task_params):
        ''' Find tasks with the given task_name and the same parameters as the kwargs
        '''
        with self._session(session) as session:
            tasks = session.query(TaskRecord).join(TaskEvent).filter(TaskRecord.name == task_name).order_by(TaskEvent.ts).all()
            for task in tasks:
                if all(k in task.parameters and v == str(task.parameters[k].value) for (k, v) in task_params.iteritems()):
                    yield task

    def find_all_by_name(self, task_name, session=None):
        ''' Find all tasks with the given task_name
        '''
        return self.find_all_by_parameters(task_name, session)

    def find_latest_runs(self, session=None):
        ''' Return tasks that have been updated in the past 24 hours.
        '''
        with self._session(session) as session:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            return session.query(TaskRecord).\
                join(TaskEvent).\
                filter(TaskEvent.ts >= yesterday).\
                group_by(TaskRecord.id, TaskEvent.event_name).\
                order_by(TaskEvent.ts.desc()).\
                all()

    def find_task_by_id(self, id, session=None):
        ''' Find task with the given record ID
        '''
        with self._session(session) as session:
            return session.query(TaskRecord).get(id)


class TaskParameter(Base):
    """ Table to track luigi.Parameter()s of a Task
    """
    __tablename__ = 'task_parameters'
    task_id = Column(Integer, ForeignKey('tasks.id'), primary_key=True)
    name = Column(String, primary_key=True)
    value = Column(String)

    def __repr__(self):
        return "TaskParameter(task_id=%d, name=%s, value=%s)" % (self.task_id, self.name, self.value)


class TaskEvent(Base):
    """ Table to track when a task is scheduled, starts, finishes, and fails
    """
    __tablename__ = 'task_events'
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('tasks.id'))
    event_name = Column(String)
    ts = Column(TIMESTAMP, index=True)

    def __repr__(self):
        return "TaskEvent(task_id=%s, event_name=%s, ts=%s" % (self.task_id, self.event_name, self.ts)


class TaskRecord(Base):
    """ Base table to track information about a luigi.Task. References to other tables are available through
    task.events, task.parameters, etc.
    """
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    host = Column(String)
    parameters = relationship('TaskParameter', collection_class=attribute_mapped_collection('name'),
                              cascade="all, delete-orphan")
    events = relationship("TaskEvent", order_by=lambda: TaskEvent.ts.desc(), backref="task")

    def __repr__(self):
        return "TaskRecord(name=%s, host=%s)" % (self.name, self.host)
