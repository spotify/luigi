import datetime
import logging

from luigi import parameter


logger = logging.getLogger('luigi-interface')


def previous(task):
    """Return a previous Task of the same family.

    By default checks if this task family only has one non-global parameter and if
    it is a DateParameter, DateHourParameter or DateIntervalParameter in which case
    it returns with the time decremented by 1 (hour, day or interval)
    """
    params = task.get_nonglobal_params()
    previous_params = {}
    previous_date_params = {}

    for param_name, param_obj in params:
        param_value = getattr(task, param_name)

        if isinstance(param_obj, parameter.DateParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(days=1)
        elif isinstance(param_obj, parameter.DateHourParameter):
            previous_date_params[param_name] = param_value - datetime.timedelta(hours=1)
        elif isinstance(param_obj, parameter.DateIntervalParameter):
            previous_date_params[param_name] = param_value.prev()
        else:
            previous_params[param_name] = param_value

    previous_params.update(previous_date_params)

    if len(previous_date_params) == 0:
        raise NotImplementedError("No task parameter - can't determine previous task")
    elif len(previous_date_params) > 1:
        raise NotImplementedError("Too many date-related task parameters - can't determine previous task")
    else:
        return task.__class__(**previous_params)


def get_previous_completed(task, max_steps=10):
    prev = task
    for i in xrange(max_steps):
        prev = previous(prev)
        logger.debug("Checking if %s is complete" % prev.task_id)
        if prev.complete():
            return prev
    return None

