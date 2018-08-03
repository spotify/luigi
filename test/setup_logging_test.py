from luigi.setup_logging import DaemonLogging, InterfaceLogging
from luigi.configuration import LuigiTomlParser
from helpers import unittest


class TestDaemonLogging(unittest.TestCase):
    cls = DaemonLogging

    def setUp(self):
        self.cls.configured = False

    def tearDown(self):
        self.cls.configured = False

    def test_cli(self):
        opts = type('opts', (), {})

        opts.background = True
        result = self.cls._cli(opts)
        self.assertTrue(result)

        opts.background = False
        opts.logdir = './tests/'
        result = self.cls._cli(opts)
        self.assertTrue(result)

        opts.background = False
        opts.logdir = False
        result = self.cls._cli(opts)
        self.assertFalse(result)

    def test_toml(self):
        self.cls.config = {'logging': {
            'version': 1,
            'disable_existing_loggers': False,
        }}
        result = self.cls._toml(None)
        self.assertTrue(result)

        self.cls.config = {}
        result = self.cls._toml(None)
        self.assertFalse(result)

    def test_cfg(self):
        self.cls.config = LuigiTomlParser()
        self.cls.config.data = {}
        result = self.cls._conf(None)
        self.assertFalse(result)

        self.cls.config.data = {'core': {'logging_conf_file': './blah'}}
        with self.assertRaises(OSError):
            self.cls._conf(None)

        self.cls.config.data = {'core': {
            'logging_conf_file': './test/testconfig/logging.cfg',
        }}
        result = self.cls._conf(None)
        self.assertTrue(result)

    def test_default(self):
        result = self.cls._default(None)
        self.assertTrue(result)
