import luigi
from luigi.mock import MockFile
import unittest

File = MockFile

class A(luigi.Task):
    def output(self):
        return File('/tmp/a.txt')

    def run(self):
        f = self.output().open('w')
        print >>f, 'hello, world'
        f.close()

class B(luigi.Task):
    def output(self):
        return File('/tmp/b.txt')

    def run(self):
        f = self.output().open('w')
        print >>f, 'goodbye, space'
        f.close()

def make_xml_wrapper(dep_class, output_filename):
    class XMLWrapper(luigi.Task):
        def output(self):
            return File(output_filename)

        def requires(self):
            return dep_class()

        def run(self):
            f = self.input().open('r')
            g = self.output().open('w')
            print >>g, '<?xml version="1.0" ?>'
            for line in f:
                print >>g, '<dummy-xml>' + line.strip() + '</dummy-xml>'
            g.close()

    return XMLWrapper

@luigi.expose
class AXML(make_xml_wrapper(A, '/tmp/a.xml')):
    pass

@luigi.expose
class BXML(make_xml_wrapper(B, '/tmp/b.xml')):
    pass

class WrapperTest(unittest.TestCase):
    def test_a(self):
        luigi.run(['--local-scheduler', 'AXML'])
        self.assertEqual(MockFile._file_contents['/tmp/a.xml'], '<?xml version="1.0" ?>\n<dummy-xml>hello, world</dummy-xml>\n')

    def test_b(self):
        luigi.run(['--local-scheduler', 'BXML'])
        self.assertEqual(MockFile._file_contents['/tmp/b.xml'], '<?xml version="1.0" ?>\n<dummy-xml>goodbye, space</dummy-xml>\n')


if __name__ == '__main__':
    luigi.run()
