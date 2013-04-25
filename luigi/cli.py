import luigi.server
import luigi.process
import optparse


def luigid():
    parser = optparse.OptionParser()
    parser.add_option('--background', help='Run in background mode', action='store_true')
    parser.add_option('--pidfile', default='luigid.pid', help='Write pidfile')
    parser.add_option('--logfile', default='luigi-server.log',
                      help='Server log file')

    opts, args = parser.parse_args()

    if opts.background:
        luigi.process.daemonize(luigi.server.run, logfile=opts.logfile,
                                pidfile=opts.pidfile)
    else:
        luigi.server.run()
