# -*- coding: utf-8 -*-
"""
This module parses commands exactly the same as the luigi task runner. You must specify the module, the task and task paramters.
Instead of executing a task, this module prints the significant paramters and state of the task and its dependencies in a tree format.
Use this to visualize the execution plan in the terminal.

.. code-block:: none

    $ luigi-deps-tree --module foo_complex examples.Foo
    ...
    └─--[Foo-{} (PENDING)]
       |--[Bar-{'num': '0'} (PENDING)]
       |  |--[Bar-{'num': '4'} (PENDING)]
       |  └─--[Bar-{'num': '5'} (PENDING)]
       |--[Bar-{'num': '1'} (PENDING)]
       └─--[Bar-{'num': '2'} (PENDING)]
          └─--[Bar-{'num': '6'} (PENDING)]
             |--[Bar-{'num': '7'} (PENDING)]
             |  |--[Bar-{'num': '9'} (PENDING)]
             |  └─--[Bar-{'num': '10'} (PENDING)]
             |     └─--[Bar-{'num': '11'} (PENDING)]
             └─--[Bar-{'num': '8'} (PENDING)]
                └─--[Bar-{'num': '12'} (PENDING)]
"""

from luigi.task import flatten
from luigi.cmdline_parser import CmdlineParser
import sys
import warnings


class bcolors:
    '''
    colored output for task status
    '''
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    ENDC = '\033[0m'


def print_tree(task, indent='', last=True):
    '''
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format
    '''
    # dont bother printing out warnings about tasks with no output
    with warnings.catch_warnings():
        warnings.filterwarnings(action='ignore', message='Task .* without outputs has no custom complete\\(\\) method')
        is_task_complete = task.complete()
    is_complete = (bcolors.OKGREEN + 'COMPLETE' if is_task_complete else bcolors.OKBLUE + 'PENDING') + bcolors.ENDC
    name = task.__class__.__name__
    params = task.to_str_params(only_significant=True)
    result = '\n' + indent
    if(last):
        result += '└─--'
        indent += '   '
    else:
        result += '|--'
        indent += '|  '
    result += '[{0}-{1} ({2})]'.format(name, params, is_complete)
    children = flatten(task.requires())
    for index, child in enumerate(children):
        result += print_tree(child, indent, (index+1) == len(children))
    return result


def main():
    cmdline_args = sys.argv[1:]
    with CmdlineParser.global_instance(cmdline_args) as cp:
        task = cp.get_task_obj()
        print(print_tree(task))


if __name__ == '__main__':
    main()
