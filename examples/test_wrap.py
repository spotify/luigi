import datetime, os
from spotify import luigi

class A(luigi.Task):
    def output(self):
        return luigi.File('/tmp/a.txt')

    def run(self):
        f = self.output().open('w')
        print >>f, 'hello, world'
        f.close()

class B(luigi.Task):
    def output(self):
        return luigi.File('/tmp/b.txt')

    def run(self):
        f = self.output().open('w')
        print >>f, 'goodbye, space'
        f.close()

def make_xml_wrapper(dep_class, output_filename):
    class XMLWrapper(luigi.Task):
        def output(self):
            return luigi.File(output_filename)

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

if __name__ == '__main__':
    luigi.run()
