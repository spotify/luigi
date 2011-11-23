import datetime, os
from spotify import builder3

class A(builder3.Task):
    def output(self):
        return builder3.File('/tmp/a.txt')

    def run(self):
        f = self.output().open('w')
        print >>f, 'hello, world'
        f.close()

class B(builder3.Task):
    def output(self):
        return builder3.File('/tmp/b.txt')

    def run(self):
        f = self.output().open('w')
        print >>f, 'goodbye, space'
        f.close()

def make_xml_wrapper(dep_class, output_filename):
    class XMLWrapper(builder3.Task):
        def output(self):
            return builder3.File(output_filename)

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

@builder3.expose
class AXML(make_xml_wrapper(A, '/tmp/a.xml')):
    pass

@builder3.expose
class BXML(make_xml_wrapper(B, '/tmp/b.xml')):
    pass

if __name__ == '__main__':
    builder3.run()
