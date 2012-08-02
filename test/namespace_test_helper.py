import luigi

luigi.namespace("mynamespace")


class Foo(luigi.Task):
    p = luigi.Parameter()


class Bar(Foo):
    task_namespace = "othernamespace"  # namespace override

luigi.namespace()
