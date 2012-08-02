import luigi
import unittest


class Foo(luigi.Task):
    pass


import namespace_test_helper  # declares another Foo in namespace mynamespace


class FooSubclass(Foo):
    pass


class TestNamespacing(unittest.TestCase):
    def test_vanilla(self):
        self.assertEquals(Foo.task_namespace, None)
        self.assertEquals(Foo.task_family, "Foo")
        self.assertEquals(Foo().task_id, "Foo()")

        self.assertEquals(FooSubclass.task_namespace, None)
        self.assertEquals(FooSubclass.task_family, "FooSubclass")
        self.assertEquals(FooSubclass().task_id, "FooSubclass()")

    def test_namespace(self):
        self.assertEquals(namespace_test_helper.Foo.task_namespace, "mynamespace")
        self.assertEquals(namespace_test_helper.Foo.task_family, "mynamespace.Foo")
        self.assertEquals(namespace_test_helper.Foo(1).task_id, "mynamespace.Foo(p=1)")

        self.assertEquals(namespace_test_helper.Bar.task_namespace, "othernamespace")
        self.assertEquals(namespace_test_helper.Bar.task_family, "othernamespace.Bar")
        self.assertEquals(namespace_test_helper.Bar(1).task_id, "othernamespace.Bar(p=1)")
