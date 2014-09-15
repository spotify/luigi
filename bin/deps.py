#!/usr/bin/env python

#
# Finds all tasks on all dependency paths from the given upstream task T down
# to the given sink task S.
#
# This is useful and practical way to find all upstream tasks of task S.
# For example suppose you have a daily computation that starts with a task named Daily.
# And suppose you have another task named Aggregate. Daily triggers a few tasks
# which eventually trigger Aggregate. Now, suppose you find a bug in Aggregate.
# You fixed the bug and now you want to rerun it, including all it's upstream deps.
# To do that you run:
#      bin/deps.py --module daily_module Daily --daily-param1 xxx --sink Aggregate
# This will output all the tasks on the dependency path b/w Daily and Aggregate. In
# effect, this is how you find all upstream tasks for Aggregate. Now you can delete its
# output and run Daily again. Daily will eventually trigget Aggregate and all tasks on
# the way.
#
# The same code here might be used as a CLI tool as well as a python module.
# In python, invoke find_deps(task, sink_name) to get a set of all task instances on the
# paths b/w task and sink. You can then use the task instances to delete their output or
# perform other computation based on that.
#
# Example:
#
# PYTHONPATH=/path/to/your/luigi/tasks bin/deps.py \
#  --module my.tasks MyUpstreamTask
#  --upstream-task-param1 123456
#  --sink MyDownstreamTask
#

from luigi.task import flatten
import luigi.interface


def get_task_requires(task):
    return set(flatten(task.requires()))


def dfs_paths(start_task, goal_task_name, path=None):
    if path is None:
        path = [start_task]
    if start_task.__class__.__name__ == goal_task_name:
        for item in path:
            yield item
    for next in get_task_requires(start_task) - set(path):
        for t in dfs_paths(next, goal_task_name, path + [next]):
            yield t


class SinkArg(luigi.Task):
    'Used to provide the global parameter --sink'
    sink = luigi.Parameter(is_global=True, default=None)


def find_deps(task, sink_task_name):
    '''
    Finds all dependencies that start with the given task and have a path
    to sink_task_name

    Returns all deps on all paths b/w task and sink
    '''
    return set([t for t in dfs_paths(task, sink_task_name)])


def find_deps_cli():
    '''
    Finds all tasks on all paths from provided CLI task and down to the
    task provided by --sink
    '''
    interface = luigi.interface.DynamicArgParseInterface()
    tasks = interface.parse()
    task, = tasks
    sink = SinkArg().sink
    return find_deps(task, sink)


if __name__ == '__main__':
    deps = find_deps_cli()
    for d in deps:
        print d
