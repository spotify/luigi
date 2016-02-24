import os
import argparse
import logging
import sys

from luigi.retcodes import run_with_retcodes


def luigi_run(argv=sys.argv[1:]):
    run_with_retcodes(argv)


def luigid(argv=sys.argv[1:]):
    import luigi.server
    import luigi.process
    import luigi.configuration
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
        luigi.server.run(api_port=opts.port, address=opts.address, unix_socket=opts.unix_socket)
