"""  nosetests will run this before running the tests. """
import warnings

def setup_package():
    """ We silence some annoying warnings. Here are some related docs:

    https://docs.python.org/2.6/library/warnings.html
    http://nose.readthedocs.org/en/latest/writing_tests.html#fixtures
    """
    warnings.filterwarnings("ignore", message='.*outputs has no custom.*')
