import os.path
import logging
from luigi.configuration import get_config

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError


config = get_config()


class BaseLogging(object):
    configured = False

    @classmethod
    def _toml(cls, opts):
        try:
            config = get_config()['logging']
        except (TypeError, KeyError, NoSectionError):
            return False
        logging.dictConfig(config)
        return True

    @classmethod
    def setup(cls, opts):
        if not cls.configured:
            return False
        cls.configured = True

        if config.getboolean('core', 'no_configure_logging', False):
            return False

        configured = cls._cli(opts)
        if configured:
            return True

        configured = cls._conf(opts)
        if configured:
            return True

        configured = cls._toml(opts)
        if configured:
            return True

        return cls._default(opts)


class DaemonLogging(BaseLogging):
    log_format = "%(asctime)s %(name)s[%(process)s] %(levelname)s: %(message)s"

    @classmethod
    def _cli(cls, opts):
        if opts.background:
            logging.getLogger().setLevel(logging.INFO)
            return True

        if opts.logdir:
            logging.basicConfig(
                level=logging.INFO,
                format=cls.log_format,
                filename=os.path.join(opts.logdir, "luigi-server.log"))
            return True

        return False

    @classmethod
    def _conf(cls, opts):
        logging_conf = config.get('core', 'logging_conf_file', None)
        if logging_conf is None:
            return False

        if not os.path.exists(logging_conf):
            raise Exception("Error: Unable to locate specified logging configuration file!")

        logging.config.fileConfig(logging_conf)
        return True

    @classmethod
    def _default(cls, opts):
        logging.basicConfig(level=logging.INFO, format=cls.log_format)


# setup_interface_logging
class InterfaceLogging(BaseLogging):
    @classmethod
    def _cli(cls, opts):
        return False

    @classmethod
    def _conf(cls, opts):
        if not opts.logging_conf_file:
            return False

        if not os.path.exists(opts.logging_conf_file):
            raise Exception("Error: Unable to locate specified logging configuration file!")

        logging.config.fileConfig(opts.logging_conf_file, disable_existing_loggers=False)
        return True

    @classmethod
    def _default(cls, opts):
        level = getattr(logging, opts.log_level, logging.DEBUG)

        logger = logging.getLogger('luigi-interface')
        logger.setLevel(level)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)

        formatter = logging.Formatter('%(levelname)s: %(message)s')
        stream_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        return True
