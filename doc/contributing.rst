Want to Contribute?
-------------------

Awesome! Let us know if you have any ideas. Feel free to contact x@y.com
where x = luigi and y = spotify.

Running Unit Tests
~~~~~~~~~~~~~~~~~~

1. Install required packages: ``pip install -r test/requirements.txt``
2. From the top directory, run
   `Nose <http://nose.readthedocs.org>`__: ``nosetests``

   -  To run all tests within individual files:
      ``nosetests test/parameter_test.py test/fib_test.py ...``
   -  To run named tests within individual files:
      ``nosetests -m '(testDate.*|testInt)' test/parameter_test.py ...``

Future Ideas
~~~~~~~~~~~~

-  S3/EC2 - We have some old ugly code based on Boto that could be
   integrated in a day or two.
-  Built in support for Pig/Hive.
-  Better visualization tool - the layout gets pretty messy as the
   number of tasks grows.
-  Integration with existing Hadoop frameworks like mrjob would be cool
   and probably pretty easy.
-  Better support (without much boilerplate) for unittesting specific
   Tasks
