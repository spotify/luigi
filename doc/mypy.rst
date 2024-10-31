Mypy plugin
--------------

Mypy plugin provides type checking for ``luigi.Task`` using Mypy.

Require Python 3.8 or later.

How to use
~~~~~~~~~~

Configure Mypy to use this plugin by adding the following to your ``mypy.ini`` file:

.. code:: ini

    [mypy]
    plugins = luigi.mypy

or by adding the following to your ``pyproject.toml`` file:

.. code:: toml

    [tool.mypy]
    plugins = ["luigi.mypy"]

Then, run Mypy as usual.

Examples
~~~~~~~~

For example the following code linted by Mypy:

.. code:: python

    import luigi


    class MyTask(luigi.Task):
        foo: int = luigi.IntParameter()
        bar: str = luigi.Parameter()

    MyTask(foo=1, bar='2')   # OK
    MyTask(foo='1', bar='2')  # Error: Argument 1 to "Foo" has incompatible type "str"; expected "int"
