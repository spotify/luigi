import warnings
warnings.warn("luigi.webhdfs module has been moved to luigi.contrib.webhdfs",
              DeprecationWarning)

from luigi.contrib.webhdfs import *
