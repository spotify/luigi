import datetime, os
from spotify import builder3

@builder3.expose
class Popularity(builder3.Task):
    date = builder3.DateParameter(default = datetime.date.today() - datetime.timedelta(1))

    def output(self):
        return builder3.File('/tmp/popularity/%s.txt' % self.date.strftime('%Y-%m-%d'))

    def requires(self):
        return Popularity(self.date - datetime.timedelta(1))

    def run(self):
        print 'transforming', self.input(), 'into', self.output()

        import random
        if random.random() > 0.5:
            raise # crash with 50% prob

        f = self.output().open('w')
        for line in self.input().open('r'):
            f.write(line)
        f.write('extra line for ' + self.date.strftime('%Y-%m-%d'))
        
        f.close()

if __name__ == '__main__':
    builder3.run()
