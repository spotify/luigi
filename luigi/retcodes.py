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
Module containing the logic for exit codes for the luigi binary. It's useful
when you in a programmatic way need to know if luigi actually finished the
given task, and if not why.
"""

import luigi
import sys
import logging
from luigi import IntParameter


class retcode(luigi.Config):
    """
    See the :ref:`return codes configuration section <retcode-config>`.
    """
    unhandled_exception = IntParameter(default=4,
                                       description='For scheduling errors or internal luigi errors.',
                                       )
    missing_data = IntParameter(default=0,
                                description="For when there are incomplete ExternalTask dependencies.",
                                )
    task_failed = IntParameter(default=0,
                               description='''For when a task's run() method fails.''',
                               )
    already_running = IntParameter(default=0,
                                   description='For both local --lock and luigid "lock"',
                                   )


def run_with_retcodes(argv):
    """
    Run luigi with command line parsing, but raise ``SystemExit`` with the configured exit code.

    Note: Usually you use the luigi binary directly and don't call this function yourself.

    :param argv: Should (conceptually) be ``sys.argv[1:]``
    """
    logger = logging.getLogger('luigi-interface')
    with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
        retcodes = retcode()

    worker = None
    try:
        worker = luigi.interface._run(argv)['worker']
    except luigi.interface.PidLockAlreadyTakenExit:
        sys.exit(retcodes.already_running)
    except Exception:
        # Some errors occur before logging is set up, we set it up now
        luigi.interface.setup_interface_logging()
        logger.exception("Uncaught exception in luigi")
        sys.exit(retcodes.unhandled_exception)

    task_sets = luigi.execution_summary._summary_dict(worker)
    non_empty_categories = {k: v for k, v in task_sets.items() if v}.keys()

    def has(status):
        assert status in luigi.execution_summary._ORDERED_STATUSES
        return status in non_empty_categories

    codes_and_conds = (
        (retcodes.missing_data, has('still_pending_ext')),
        (retcodes.task_failed, has('failed')),
        (retcodes.already_running, has('run_by_other_worker')),
    )
    sys.exit(max(code * (1 if cond else 0) for code, cond in codes_and_conds))
