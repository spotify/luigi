from luigi.setup_logging import DaemonLogging, InterfaceLogging
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
