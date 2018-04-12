"""
Create a luigi.cfg file with a given encoding.
You can find an example file in examples/hello_encoding_world.cfg (adapt encoding if needed)

Set the LUIGI_CONFIG_ENCODING value to the correct encoding.
Set the LUIGI_CONFIG_PATH
You can run this example like this:

    .. code:: console

            $ python examples/hello_encoding_world.py --local-scheduler

If that does not work, see :ref:`CommandLine`.
"""
import luigi


class HelloEncodingWorldTask(luigi.Task):
    localized_message = luigi.Parameter()

    def run(self):
        print("{task} says: {message}".format(task=self.__class__.__name__, message=self.localized_message))


if __name__ == '__main__':
    luigi.run(['HelloEncodingWorldTask', '--workers', '1', '--local-scheduler'])
