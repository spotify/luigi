import luigi
import unittest

@luigi.expose
class Factorial(luigi.Task):
    ''' This calculates factorials *online* and does not write its results anywhere

    Demonstrates the ability for dependencies between Tasks and not just between their output.
    '''
    n = luigi.IntParameter(default=100)

    def requires(self):
        if self.n > 1:
            return Factorial(self.n-1)

    def run(self):
        if self.n > 1:
            self.value = self.n * self.requires().value
        else:
            self.value = 1
        self.complete = lambda: True

    def complete(self):
        return False

class FibTest(unittest.TestCase):
    def test_invoke(self):
        w = luigi.worker.Worker()
        w.add(Factorial(100))
        w.run()

        self.assertEqual(Factorial(42).value, 1405006117752879898543142606244511569936384000000000)

if __name__ == '__main__':
    luigi.run()

