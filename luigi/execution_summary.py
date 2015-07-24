# -*- coding: utf-8 -*-
#
# Copyright 2015-2015 Spotify AB
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
This module provide the function :py:func:`summary` that is used for printing
an execution summary at the end of luigi invocations.

See `https://github.com/spotify/luigi/blob/master/examples/execution_summary_example.py` for an example.
"""

import textwrap
import datetime


def _partition_tasks(worker):
    """
    Takes a worker and sorts out tasks based on their status.
    Still_pending_not_ext is only used to get upstream_failure, upstream_missing_dependency and run_by_other_worker
    """
    task_history = worker._add_task_history
    pending_tasks = {task for(task, status, ext) in task_history if status == 'PENDING'}
    set_tasks = {}
    set_tasks["completed"] = {task for (task, status, ext) in task_history if status == 'DONE' and task in pending_tasks}
    set_tasks["already_done"] = {task for (task, status, ext) in task_history if status == 'DONE' and task not in pending_tasks and task not in set_tasks["completed"]}
    set_tasks["failed"] = {task for (task, status, ext) in task_history if status == 'FAILED'}
    set_tasks["still_pending_ext"] = {task for (task, status, ext) in task_history if status == 'PENDING' and task not in set_tasks["failed"] and task not in set_tasks["completed"] and not ext}
    set_tasks["still_pending_not_ext"] = {task for (task, status, ext) in task_history if status == 'PENDING' and task not in set_tasks["failed"] and task not in set_tasks["completed"] and ext}
    set_tasks["run_by_other_worker"] = set()
    set_tasks["upstream_failure"] = set()
    set_tasks["upstream_missing_dependency"] = set()
    set_tasks["upstream_run_by_other_worker"] = set()
    set_tasks["unknown_reason"] = set()
    return set_tasks


def _dfs(set_tasks, current_task, visited):
    """
    This dfs checks why tasks are still pending
    """
    visited.add(current_task)
    if current_task in set_tasks["still_pending_not_ext"]:
        upstream_failure = False
        upstream_missing_dependency = False
        upstream_run_by_other_worker = False
        for task in current_task._requires():
            if task not in visited:
                _dfs(set_tasks, task, visited)
            if task in set_tasks["failed"] or task in set_tasks["upstream_failure"]:
                set_tasks["upstream_failure"].add(current_task)
                upstream_failure = True
            if task in set_tasks["still_pending_ext"] or task in set_tasks["upstream_missing_dependency"]:
                set_tasks["upstream_missing_dependency"].add(current_task)
                upstream_missing_dependency = True
            if task in set_tasks["run_by_other_worker"] or task in set_tasks["upstream_run_by_other_worker"]:
                set_tasks["upstream_run_by_other_worker"].add(current_task)
                upstream_run_by_other_worker = True
        if not upstream_failure and not upstream_missing_dependency and not upstream_run_by_other_worker and current_task not in set_tasks["run_by_other_worker"]:
            set_tasks["unknown_reason"].add(current_task)


def _get_str(task_dict, extra_indent):
    """
    This returns a string for each status
    """
    lines = []
    for task_family, tasks in task_dict.items():
        row = '    '
        if extra_indent:
            row += '    '
        if len(lines) >= 5:
            """
            This is how many rows will be printed for each status. If you want fewer rows you can lower the limit.
            """
            row += '...'
            lines.append(row)
            break
        if len(tasks[0].get_params()) == 0:
            row += '- {0} {1}()'.format(len(tasks), str(task_family))
        elif _get_len_of_params(tasks[0]) > 60 or (len(tasks) == 2 and len(tasks[0].get_params()) > 1 and (_get_len_of_params(tasks[0]) > 40 or len(str(tasks[0])) > 100)) or len(str(tasks[0])) > 200:
            """
            This is to make sure that there is no really long task in the output
            """
            row += '- {0} {1}(...)'.format(len(tasks), task_family)
        elif len((tasks[0].get_params())) == 1:
            attributes = sorted({getattr(task, tasks[0].get_params()[0][0]) for task in tasks})
            row += '- {0} {1}({2}='.format(len(tasks), task_family, tasks[0].get_params()[0][0])
            if _ranging_attributes(attributes, tasks[0].get_params()[0]) and len(attributes) > 3:
                row += '{0}...{1}'.format(tasks[0].get_params()[0][1].serialize(attributes[0]), tasks[0].get_params()[0][1].serialize(attributes[len(attributes) - 1]))
            else:
                row += '{0}'.format(_get_str_one_parameter(tasks))
            row += ")"
        else:
            ranging = False
            params = _get_set_of_params(tasks)
            if _only_one_unique_param(params):
                unique_param = _get_unique_param(params)
                attributes = sorted(params[unique_param])
                if _ranging_attributes(attributes, unique_param) and len(attributes) > 2:
                    ranging = True
                    row += '- {0} {1}({2}'.format(len(tasks), task_family, _get_str_ranging_multiple_parameters(attributes, tasks, unique_param))
            if not ranging:
                if len(tasks) == 1:
                    row += '- {0} {1}'.format(len(tasks), tasks[0])
                if len(tasks) == 2:
                    row += '- {0} and {1}'.format(tasks[0], tasks[1])
                if len(tasks) > 2:
                    row += '- {0} and {1} other {2}'.format(tasks[0], len(tasks) - 1, task_family)
        lines.append(row)
    return '\n'.join(lines)


def _get_len_of_params(task):
    _len_of_params = 0
    for param in task.get_params():
        _len_of_params += len(param[0])
    return _len_of_params


def _get_str_ranging_multiple_parameters(attributes, tasks, unique_param):
    row = ''
    str_unique_param = '{0}...{1}'.format(unique_param[1].serialize(attributes[0]), unique_param[1].serialize(attributes[len(attributes) - 1]))
    for param in tasks[0].get_params():
        row += '{0}='.format(param[0])
        if param[0] == unique_param[0]:
            row += '{0}'.format(str_unique_param)
        else:
            row += '{0}'.format(param[1].serialize(getattr(tasks[0], param[0])))
        if param != tasks[0].get_params()[len(tasks[0].get_params()) - 1]:
            row += ", "
    row += ')'
    return row


def _get_set_of_params(tasks):
    params = {}
    for param in tasks[0].get_params():
        params[param] = {getattr(task, param[0]) for task in tasks}
    return params


def _only_one_unique_param(params):
    len_params = len(params)
    different = [1 for param in params if len(params[param]) == 1]
    len_different = len(different)
    if len_params - len_different == 1:
        return True
    else:
        return False


def _get_unique_param(params):
    for param in params:
        if len(params[param]) > 1:
            return param


def _ranging_attributes(attributes, unique_param):
    """
    Checks if there is a continuous range
    """
    ranging = False
    if len(attributes) > 2:
        ranging = True
        if unique_param[1].next_in_enumeration(attributes[0]) is None:
            ranging = False
        if ranging:
            for i in range(1, len(attributes)):
                if unique_param[1].next_in_enumeration(attributes[i - 1]) != attributes[i]:
                    ranging = False
                    break
    return ranging


def _get_str_one_parameter(tasks):
    row = ''
    count = 0
    for task in tasks:
        if (len(row) >= 30 and count > 2 and count != len(tasks) - 1) or len(row) > 200:
            row += '...'
            break
        row += '{0}'.format(getattr(task, task.get_params()[0][0]))
        if count < len(tasks) - 1:
            row += ','
        count += 1
    return row


def _serialize_first_param(task):
    return task.get_params()[0][1].serialize(getattr(task, task.get_params()[0][0]))


def _get_number_of_tasks(task_dict):
    num = 0
    for task_family, tasks in task_dict.items():
        num += len(tasks)
    return num


def _get_comments(group_tasks):
    comments = {}
    for status, task_dict in group_tasks.items():
        comments[status] = "* {0}".format(_get_number_of_tasks(task_dict))
        if _get_number_of_tasks(task_dict) == 0:
            comments.pop(status)
    if "already_done" in comments:
        comments["already_done"] += ' present dependencies were encountered:\n'
    if "completed" in comments:
        comments["completed"] += ' ran successfully:\n'
    if "failed" in comments:
        comments["failed"] += ' failed:\n'
    still_pending = False
    if "still_pending_ext" in comments:
        comments["still_pending_ext"] = '    {0} were missing external dependencies:\n'.format(comments['still_pending_ext'])
        still_pending = True
    if "upstream_run_by_other_worker" in comments:
        comments["upstream_run_by_other_worker"] = '    {0} had dependencies that were being run by other worker:\n'.format(comments['upstream_run_by_other_worker'])
        still_pending = True
    if "upstream_failure" in comments:
        comments["upstream_failure"] = '    {0} had failed dependencies:\n'.format(comments['upstream_failure'])
        still_pending = True
    if "upstream_missing_dependency" in comments:
        comments["upstream_missing_dependency"] = '    {0} had missing external dependencies:\n'.format(comments['upstream_missing_dependency'])
        still_pending = True
    if "run_by_other_worker" in comments:
        comments["run_by_other_worker"] = '    {0} were being run by another worker:\n'.format(comments['run_by_other_worker'])
        still_pending = True
    if "unknown_reason" in comments:
        comments["unknown_reason"] = '    {0} were left pending because of unknown reason:\n'.format(comments["unknown_reason"])
        still_pending = True
    if still_pending:
        comments["still_pending"] = '* {0} were left pending, among these:\n'.format(_get_number_of_tasks(group_tasks["still_pending_ext"]) + _get_number_of_tasks(group_tasks["still_pending_not_ext"]))
    return comments


def _get_statuses():
    statuses = ["already_done", "completed", "failed", "still_pending", "still_pending_ext", "run_by_other_worker", "upstream_failure", "upstream_missing_dependency", "upstream_run_by_other_worker", "unknown_reason"]
    return statuses


def _get_run_by_other_worker(worker):
    """
    This returns a set of the tasks that are being run by other worker
    """
    worker_that_blocked_task = dict()
    get_work_response_history = worker._get_work_response_history
    for get_work_response in get_work_response_history:
        if get_work_response['task_id'] is None:
            for running_task in get_work_response['running_tasks']:
                other_worker_id = running_task['worker']
                other_task_id = running_task['task_id']
                other_task = worker._scheduled_tasks.get(other_task_id)
                if other_task:
                    worker_that_blocked_task[other_task] = other_worker_id
    return set(worker_that_blocked_task.keys())


def _get_external_workers(worker):
    """
    This returns a dict with a set of tasks for all of the other workers
    """
    worker_that_blocked_task = dict()
    get_work_response_history = worker._get_work_response_history
    for get_work_response in get_work_response_history:
        if get_work_response['task_id'] is None:
            for running_task in get_work_response['running_tasks']:
                other_worker_id = running_task['worker']
                other_task_id = running_task['task_id']
                other_task = worker._scheduled_tasks.get(other_task_id)
                if other_task:
                    if other_worker_id not in worker_that_blocked_task.keys():
                        worker_that_blocked_task[other_worker_id] = set()
                    worker_that_blocked_task[other_worker_id].add(other_task)
    return worker_that_blocked_task


def _group_tasks_by_name_and_status(task_dict):
    """
    Takes a dictionary with sets of tasks grouped by their status and returns a dictionary with dictionaries with an array of tasks grouped by their status and task name
    """
    group_status = {}
    for task in task_dict:
        if task.task_family not in group_status:
            group_status[task.task_family] = []
        group_status[task.task_family].append(task)
    return group_status


def _summary_dict(worker):
    set_tasks = _partition_tasks(worker)
    set_tasks["run_by_other_worker"] = _get_run_by_other_worker(worker)
    visited = set()
    for task in set_tasks["still_pending_not_ext"]:
        _dfs(set_tasks, task, visited)
    return set_tasks


def _summary_format(set_tasks, worker):
    group_tasks = {}
    for status, task_dict in set_tasks.items():
        group_tasks[status] = _group_tasks_by_name_and_status(task_dict)
    str_tasks = {}
    comments = _get_comments(group_tasks)
    statuses = _get_statuses()
    num_all_tasks = len(set_tasks["already_done"]) + len(set_tasks["completed"]) + len(set_tasks["failed"]) + len(set_tasks["still_pending_ext"]) + len(set_tasks["still_pending_not_ext"])
    str_output = ''
    str_output += 'Scheduled {0} tasks of which:\n'.format(num_all_tasks)
    for i in range(len(statuses)):
        if statuses[i] not in comments:
            continue
        str_output += '{0}'.format(comments[statuses[i]])
        if statuses[i] != 'still_pending':
            str_output += '{0}\n'.format(_get_str(group_tasks[statuses[i]], i > 3))
    ext_workers = _get_external_workers(worker)
    group_tasks_ext_workers = {}
    for ext_worker, task_dict in ext_workers.items():
        group_tasks_ext_workers[ext_worker] = _group_tasks_by_name_and_status(task_dict)
    if len(ext_workers) > 0:
        str_output += "\nThe other workers were:\n"
        count = 0
        for ext_worker, task_dict in ext_workers.items():
            if count > 3 and count < len(ext_workers) - 1:
                str_output += "    and {0} other workers".format(len(ext_workers) - count)
                break
            str_output += "    - {0} ran {1} tasks\n".format(ext_worker, len(task_dict))
            count += 1
        str_output += '\n'
    if num_all_tasks == len(set_tasks["already_done"]) + len(set_tasks["still_pending_ext"]) + len(set_tasks["still_pending_not_ext"]):
        if len(ext_workers) == 0:
            str_output += '\n'
        str_output += 'Did not run any tasks'
    smiley = ""
    reason = ""
    if len(set_tasks["failed"]):
        smiley = ":("
        reason = "there were failed tasks"
    elif len(set_tasks["still_pending_ext"]):
        smiley = ":|"
        reason = "there were missing external dependencies"
    else:
        smiley = ":)"
        reason = "there were no failed tasks or missing external dependencies"
    str_output += "\nThis progress looks {0} because {1}".format(smiley, reason)
    if num_all_tasks == 0:
        str_output = 'Did not schedule any tasks'
    return str_output


def _summary_wrap(str_output):
    return textwrap.dedent("""
    ===== Luigi Execution Summary =====

    {str_output}

    ===== Luigi Execution Summary =====
    """).format(str_output=str_output)


def summary(worker):
    """
    Given a worker, return a human readable string describing roughly what the
    workers have done.
    """
    return _summary_wrap(_summary_format(_summary_dict(worker), worker))
# 5
