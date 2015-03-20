#!/usr/bin/env python


# Finds all tasks and task outputs on the dependency paths from the given downstream task T
# up to the given source/upstream task S (optional). If the upstream task is not given,
# all upstream tasks on all dependancy paths of T will be returned.

# Terms:
# if  the execution of Task T depends on the output of task S on a dependancy graph,
#  T is called a downstream/sink task, S is called an upstream/source task.

# This is useful and practical way to find all upstream tasks of task T.
# For example suppose you have a daily computation that starts with a task named Daily.
# And suppose you have another task named Aggregate. Daily triggers a few tasks
# which eventually trigger Aggregate. Now, suppose you find a bug in Aggregate.
# You fixed the bug and now you want to rerun it, including all it's upstream deps.
#
# To do that you run:
#      bin/deps.py --module daily_module Aggregate --daily-param1 xxx --upstream Daily
#
# This will output all the tasks on the dependency path b/w Daily and Aggregate. In
# effect, this is how you find all upstream tasks for Aggregate. Now you can delete its
# output and run Aggregate again. Daily will eventually trigget Aggregate and all tasks on
# the way.
#
# The same code here might be used as a CLI tool as well as a python module.
# In python, invoke find_deps(task, upstream_name) to get a set of all task instances on the
# paths b/w task T and upstream task S. You can then use the task instances to delete their output or
# perform other computation based on that.
#
# Example:
#
# PYTHONPATH=$PYTHONPATH:/path/to/your/luigi/tasks bin/deps.py \
# --module my.tasks  MyDownstreamTask
# --downstream_task_param1 123456
# [--upstream MyUpstreamTask]
#


import luigi.interface
from luigi.contrib.ssh import RemoteTarget
from luigi.postgres import PostgresTarget
from luigi.s3 import S3Target
from luigi.target import FileSystemTarget
from luigi.task import flatten


def get_task_requires(task):
    return set(flatten(task.requires()))


def dfs_paths(start_task, goal_task_name, path=None):
    if path is None:
        path = [start_task]
    if start_task.__class__.__name__ == goal_task_name or goal_task_name is None:
        for item in path:
            yield item
    for next in get_task_requires(start_task) - set(path):
        for t in dfs_paths(next, goal_task_name, path + [next]):
            yield t


class UpstreamArg(luigi.Task):

    'Used to provide the global parameter -- upstream'
    upstream = luigi.Parameter(is_global=True, default=None)


def find_deps(task, upstream_task_name):
    '''
    Finds all dependencies that start with the given task and have a path
    to upstream_task_name

    Returns all deps on all paths b/w task and upstream
    '''
    return set([t for t in dfs_paths(task, upstream_task_name)])


def find_deps_cli():
    '''
    Finds all tasks on all paths from provided CLI task and down to the
    task provided by --upstream
    '''
    interface = luigi.interface.DynamicArgParseInterface()
    tasks = interface.parse()
    task, = tasks
    upstream = UpstreamArg().upstream
    return find_deps(task, upstream)


def main():
    deps = find_deps_cli()
    for d in deps:
        task_name = d
        task_output = u"n/a"
        if isinstance(d.output(), RemoteTarget):
            task_output = u"[SSH] {0}:{1}".format(d.output()._fs.remote_context.host, d.output().path)
        elif isinstance(d.output(), S3Target):
            task_output = u"[S3] {0}".format(d.output().path)
        elif isinstance(d.output(), FileSystemTarget):
            task_output = u"[FileSystem] {0}".format(d.output().path)
        elif isinstance(d.output(), PostgresTarget):
            task_output = u"[DB] {0}:{1}".format(d.output().host, d.output().table)
        else:
            task_output = "to be determined"
        print(u"""   TASK: {0}
                       : {1}""".format(task_name, task_output))


if __name__ == '__main__':
    main()
