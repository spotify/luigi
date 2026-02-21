import argparse
import sys

from luigi.retcodes import run_with_retcodes
from luigi.setup_logging import DaemonLogging


def luigi_run(argv=sys.argv[1:]):
    run_with_retcodes(argv)


def luigid(argv=sys.argv[1:]):
    import luigi.configuration
    import luigi.process
    import luigi.server

    parser = argparse.ArgumentParser(description="Central luigi server")
    parser.add_argument("--background", help="Run in background mode", action="store_true")
    parser.add_argument("--pidfile", help="Write pidfile")
    parser.add_argument("--logdir", help="log directory")
    parser.add_argument("--state-path", help="Pickled state file")
    parser.add_argument("--address", help="Listening interface")
    parser.add_argument("--unix-socket", help="Unix socket path")
    parser.add_argument("--port", default=8082, help="Listening port")

    opts = parser.parse_args(argv)

    if opts.state_path:
        config = luigi.configuration.get_config()
        config.set("scheduler", "state_path", opts.state_path)

    DaemonLogging.setup(opts)
    if opts.background:
        luigi.process.daemonize(
            luigi.server.run, api_port=opts.port, address=opts.address, pidfile=opts.pidfile, logdir=opts.logdir, unix_socket=opts.unix_socket
        )
    else:
        luigi.server.run(api_port=opts.port, address=opts.address, unix_socket=opts.unix_socket)
