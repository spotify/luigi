import os
import argparse
import logging
import sys

import luigi.interface
import luigi.server
import luigi.process
import luigi.configuration
import luigi.interface


class cmdline(luigi.Config):
    use_cmdline_section = False
    retcode_internal_failure = luigi.IntParameter(default=1)
    retcode_missing_data = luigi.IntParameter(default=0)
    retcode_task_failed = luigi.IntParameter(default=0)
    retcode_already_running = luigi.IntParameter(default=0, description='For both local --lock and luigid "lock"')


def luigi_run(argv=sys.argv[1:]):
    """
    Return
    """
    logger = logging.getLogger('luigi-interface')
    with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
        cmdl = cmdline()

    try:
        luigi.interface.run(argv)
    except Exception:
        logger.exception("Unknown uncaught exception in luigi")
        sys.exit(cmdl.retcode_internal_failure)

    worker = luigi.w  # lulz
    task_sets = luigi.execution_summary._summary_dict(worker)
    non_empty_categories = {k: v for k, v in task_sets.items() if v}.keys()

    def has(status):
        assert status in luigi.execution_summary._ORDERED_STATUSES
        return status in non_empty_categories

    codes_and_conds = (
        (cmdl.retcode_missing_data, has('still_pending_ext')),
        (cmdl.retcode_task_failed, has('failed')),
        (cmdl.retcode_already_running, has('run_by_other_worker')),  # TODO: lockfile logic
    )
    sys.exit(max(code * (1 if cond else 0) for code, cond in codes_and_conds))


def luigid(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description=u'Central luigi server')
    parser.add_argument(u'--background', help=u'Run in background mode', action='store_true')
    parser.add_argument(u'--pidfile', help=u'Write pidfile')
    parser.add_argument(u'--logdir', help=u'log directory')
    parser.add_argument(u'--state-path', help=u'Pickled state file')
    parser.add_argument(u'--address', help=u'Listening interface')
    parser.add_argument(u'--unix-socket', help=u'Unix socket path')
    parser.add_argument(u'--port', default=8082, help=u'Listening port')

    opts = parser.parse_args(argv)

    if opts.state_path:
        config = luigi.configuration.get_config()
        config.set('scheduler', 'state_path', opts.state_path)

    if opts.background:
        # daemonize sets up logging to spooled log files
        logging.getLogger().setLevel(logging.INFO)
        luigi.process.daemonize(luigi.server.run, api_port=opts.port,
                                address=opts.address, pidfile=opts.pidfile,
                                logdir=opts.logdir, unix_socket=opts.unix_socket)
    else:
        if opts.logdir:
            logging.basicConfig(level=logging.INFO, format=luigi.process.get_log_format(),
                                filename=os.path.join(opts.logdir, "luigi-server.log"))
        else:
            logging.basicConfig(level=logging.INFO, format=luigi.process.get_log_format())
        luigi.server.run(api_port=opts.port, address=opts.address)
