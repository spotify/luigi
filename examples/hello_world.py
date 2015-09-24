import luigi


class HelloWorldTask(luigi.Task):
    task_namespace = 'examples'

    def run(self):
        print("{task} says: Hello world!".format(task=self.__class__.__name__))

if __name__ == '__main__':
    luigi.run(['examples.HelloWorldTask', '--workers', '1', '--local-scheduler'])
