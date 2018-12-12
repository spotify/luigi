from luigi.setup_logging import DaemonLogging, InterfaceLogging
from luigi.configuration import LuigiTomlParser, LuigiConfigParser, get_config
from helpers import unittest


class TestDaemonLogging(unittest.TestCase):
    cls = DaemonLogging

    def setUp(self):
        self.cls._configured = False

    def tearDown(self):
        self.cls._configured = False
        self.cls.config = get_config()

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

    def test_section(self):
        self.cls.config = {'logging': {
            'version': 1,
            'disable_existing_loggers': False,
        }}
        result = self.cls._section(None)
        self.assertTrue(result)

        self.cls.config = {}
        result = self.cls._section(None)
        self.assertFalse(result)

    def test_section_cfg(self):
        self.cls.config = LuigiConfigParser.instance()
        result = self.cls._section(None)
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


class TestInterfaceLogging(TestDaemonLogging):
    cls = InterfaceLogging

    def test_cli(self):
        opts = type('opts', (), {})
        result = self.cls._cli(opts)
        self.assertFalse(result)

    # test_section inherited from TestDaemonLogging

    def test_cfg(self):
        self.cls.config = LuigiTomlParser()
        self.cls.config.data = {}

        opts = type('opts', (), {})
        opts.logging_conf_file = ''
        result = self.cls._conf(opts)
        self.assertFalse(result)

        opts.logging_conf_file = './blah'
        with self.assertRaises(OSError):
            self.cls._conf(opts)

        opts.logging_conf_file = './test/testconfig/logging.cfg'
        result = self.cls._conf(opts)
        self.assertTrue(result)

    def test_default(self):
        opts = type('opts', (), {})
        opts.log_level = 'INFO'
        result = self.cls._default(opts)
        self.assertTrue(result)


class PatchedLogging(InterfaceLogging):

    @classmethod
    def _cli(cls, *args):
        cls.calls.append('_cli')
        return '_cli' not in cls.patched

    @classmethod
    def _conf(cls, *args):
        cls.calls.append('_conf')
        return '_conf' not in cls.patched

    @classmethod
    def _section(cls, *args):
        cls.calls.append('_section')
        return '_section' not in cls.patched

    @classmethod
    def _default(cls, *args):
        cls.calls.append('_default')
        return '_default' not in cls.patched


class TestSetup(unittest.TestCase):
    def setUp(self):
        self.opts = type('opts', (), {})
        self.cls = PatchedLogging
        self.cls.calls = []
        self.cls.config = LuigiTomlParser()
        self.cls._configured = False
        self.cls.patched = '_cli', '_conf', '_section', '_default'

    def tearDown(self):
        self.cls.config = get_config()

    def test_configured(self):
        self.cls._configured = True
        result = self.cls.setup(self.opts)
        self.assertEqual(self.cls.calls, [])
        self.assertFalse(result)

    def test_disabled(self):
        self.cls.config.data = {'core': {'no_configure_logging': True}}
        result = self.cls.setup(self.opts)
        self.assertEqual(self.cls.calls, [])
        self.assertFalse(result)

    def test_order(self):
        self.cls.setup(self.opts)
        self.assertEqual(self.cls.calls, ['_cli', '_conf', '_section', '_default'])

    def test_cli(self):
        self.cls.patched = ()
        result = self.cls.setup(self.opts)
        self.assertTrue(result)
        self.assertEqual(self.cls.calls, ['_cli'])

    def test_conf(self):
        self.cls.patched = ('_cli', )
        result = self.cls.setup(self.opts)
        self.assertTrue(result)
        self.assertEqual(self.cls.calls, ['_cli', '_conf'])

    def test_section(self):
        self.cls.patched = ('_cli', '_conf')
        result = self.cls.setup(self.opts)
        self.assertTrue(result)
        self.assertEqual(self.cls.calls, ['_cli', '_conf', '_section'])

    def test_default(self):
        self.cls.patched = ('_cli', '_conf', '_section')
        result = self.cls.setup(self.opts)
        self.assertTrue(result)
        self.assertEqual(self.cls.calls, ['_cli', '_conf', '_section', '_default'])
