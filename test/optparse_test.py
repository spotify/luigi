import luigi
from luigi.mock import MockFile
from fib_test import FibTestBase

class OptParseTest(FibTestBase):
    def test_cmdline_optparse(self):
        luigi.run(['--local-scheduler', '--task', 'Fib', '--n', '100'], use_optparse=True)

        self.assertEqual(MockFile._file_contents['/tmp/fib_10'], '55\n')
        self.assertEqual(MockFile._file_contents['/tmp/fib_100'], '354224848179261915075\n')

    def test_cmdline_optparse_existing(self):
        import optparse
        parser = optparse.OptionParser()
        parser.add_option('--blaha')

        luigi.run(['--local-scheduler', '--task', 'Fib', '--n', '100'], use_optparse=True, existing_optparse=parser)

        self.assertEqual(MockFile._file_contents['/tmp/fib_10'], '55\n')
        self.assertEqual(MockFile._file_contents['/tmp/fib_100'], '354224848179261915075\n')
