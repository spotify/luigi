# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Provides a database backend to the central scheduler. This lets you see historical runs.
See :ref:`TaskHistory` for information about how to turn out the task history feature.
"""
#
# Description: Added codes for visualization of how long each task takes
# running-time until it reaches the next status (failed or done)
# At "{base_url}/tasklist", all completed(failed or done) tasks are shown.
# At "{base_url}/tasklist", a user can select one specific task to see
# how its running-time has changed over time.
# At "{base_url}/tasklist/{task_name}", it visualizes a multi-bar graph
# that represents the changes of the running-time for a selected task
# up to the next status (failed or done).
# This visualization let us know how the running-time of the specific task
# has changed over time.
#
# Copyright 2015 Naver Corp.
# Author Yeseul Park (yeseul.park@navercorp.com)
#

import datetime
import logging
from contextlib import contextmanager

from luigi import six

from luigi import configuration
from luigi import task_history
from luigi.task_status import DONE, FAILED, PENDING, RUNNING

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.collections
from sqlalchemy.engine import reflection
Base = sqlalchemy.ext.declarative.declarative_base()

logger = logging.getLogger('luigi-interface')


class DbTaskHistory(task_history.TaskHistory):
    """
    Task History that writes to a database using sqlalchemy.
    Also has methods for useful db queries.
    """
    CURRENT_SOURCE_VERSION = 1

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
        self.engine = sqlalchemy.create_engine(connection_string)
        self.session_factory = sqlalchemy.orm.sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)
        self.tasks = {}  # task_id -> TaskRecord

        _upgrade_schema(self.engine)

    def task_scheduled(self, task):
        htask = self._get_task(task, status=PENDING)
        self._add_task_event(htask, TaskEvent(event_name=PENDING, ts=datetime.datetime.now()))

    def task_finished(self, task, successful):
        event_name = DONE if successful else FAILED
        htask = self._get_task(task, status=event_name)
        self._add_task_event(htask, TaskEvent(event_name=event_name, ts=datetime.datetime.now()))

    def task_started(self, task, worker_host):
        htask = self._get_task(task, status=RUNNING, host=worker_host)
        self._add_task_event(htask, TaskEvent(event_name=RUNNING, ts=datetime.datetime.now()))

    def _get_task(self, task, status, host=None):
        if task.id in self.tasks:
            htask = self.tasks[task.id]
            htask.status = status
            if host:
                htask.host = host
        else:
            htask = self.tasks[task.id] = task_history.StoredTask(task, status, host)
        return htask

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
                task_record = TaskRecord(task_id=task._task.id, name=task.task_family, host=task.host)
                for (k, v) in six.iteritems(task.parameters):
                    task_record.parameters[k] = TaskParameter(name=k, value=v)
                session.add(task_record)
                yield (task_record, session)
            if task.host:
                task_record.host = task.host
        task.record_id = task_record.id

    def find_all_by_parameters(self, task_name, session=None, **task_params):
        """
        Find tasks with the given task_name and the same parameters as the kwargs.
        """
        with self._session(session) as session:
            query = session.query(TaskRecord).join(TaskEvent).filter(TaskRecord.name == task_name)
            for (k, v) in six.iteritems(task_params):
                alias = sqlalchemy.orm.aliased(TaskParameter)
                query = query.join(alias).filter(alias.name == k, alias.value == v)

            tasks = query.order_by(TaskEvent.ts)
            for task in tasks:
                # Sanity check
                assert all(k in task.parameters and v == str(task.parameters[k].value) for (k, v) in six.iteritems(task_params))

                yield task

    def find_all_by_name(self, task_name, session=None):
        """
        Find all tasks with the given task_name.
        """
        return self.find_all_by_parameters(task_name, session)

    def find_latest_runs(self, session=None):
        """
        Return tasks that have been updated in the past 24 hours.
        """
        with self._session(session) as session:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            return session.query(TaskRecord).\
                join(TaskEvent).\
                filter(TaskEvent.ts >= yesterday).\
                group_by(TaskRecord.id, TaskEvent.event_name, TaskEvent.ts).\
                order_by(TaskEvent.ts.desc()).\
                all()

    def find_all_runs(self, session=None):
        """
        Return all tasks that have been updated.
        """
        with self._session(session) as session:
            return session.query(TaskRecord).all()

    def find_all_events(self, session=None):
        """
        Return all running/failed/done events.
        """
        with self._session(session) as session:
            return session.query(TaskEvent).all()

    def find_task_by_id(self, id, session=None):
        """
        Find task with the given record ID.
        """
        with self._session(session) as session:
            return session.query(TaskRecord).get(id)


class TaskParameter(Base):
    """
    Table to track luigi.Parameter()s of a Task.
    """
    __tablename__ = 'task_parameters'
    task_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('tasks.id'), primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String(128), primary_key=True)
    value = sqlalchemy.Column(sqlalchemy.String(256))

    def __repr__(self):
        return "TaskParameter(task_id=%d, name=%s, value=%s)" % (self.task_id, self.name, self.value)


class TaskEvent(Base):
    """
    Table to track when a task is scheduled, starts, finishes, and fails.
    """
    __tablename__ = 'task_events'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    task_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('tasks.id'), index=True)
    event_name = sqlalchemy.Column(sqlalchemy.String(20))
    ts = sqlalchemy.Column(sqlalchemy.TIMESTAMP, index=True, nullable=False)

    def __repr__(self):
        return "TaskEvent(task_id=%s, event_name=%s, ts=%s" % (self.task_id, self.event_name, self.ts)


class TaskRecord(Base):
    """
    Base table to track information about a luigi.Task.

    References to other tables are available through task.events, task.parameters, etc.
    """
    __tablename__ = 'tasks'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    task_id = sqlalchemy.Column(sqlalchemy.String(200), index=True)
    name = sqlalchemy.Column(sqlalchemy.String(128), index=True)
    host = sqlalchemy.Column(sqlalchemy.String(128))
    parameters = sqlalchemy.orm.relationship(
        'TaskParameter',
        collection_class=sqlalchemy.orm.collections.attribute_mapped_collection('name'),
        cascade="all, delete-orphan")
    events = sqlalchemy.orm.relationship(
        'TaskEvent',
        order_by=(sqlalchemy.desc(TaskEvent.ts), sqlalchemy.desc(TaskEvent.id)),
        backref='task')

    def __repr__(self):
        return "TaskRecord(name=%s, host=%s)" % (self.name, self.host)


def _upgrade_schema(engine):
    """
    Ensure the database schema is up to date with the codebase.

    :param engine: SQLAlchemy engine of the underlying database.
    """
    inspector = reflection.Inspector.from_engine(engine)
    conn = engine.connect()

    # Upgrade 1.  Add task_id column and index to tasks
    if 'task_id' not in [x['name'] for x in inspector.get_columns('tasks')]:
        logger.warn('Upgrading DbTaskHistory schema: Adding tasks.task_id')
        conn.execute('ALTER TABLE tasks ADD COLUMN task_id VARCHAR(200)')
        conn.execute('CREATE INDEX ix_task_id ON tasks (task_id)')
