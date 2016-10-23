import os
import tempfile
import unittest

from luigi.contrib.hdf5 import Hdf5TableTarget, AtomicHdf5Table
from nose.plugins.attrib import attr

try:
    import numpy as np
    import pandas.util.testing as pdt
    import pandas as pd
except ImportError:
    raise unittest.SkipTest("unable to load pandas module")


def cols_sorted(df):
    l = list(df.columns)
    return all([l[i] <= l[i + 1] for i in range(len(l) - 1)])


@attr('hdf5')
class Hdf5TableTargetTest(unittest.TestCase):
    def setUp(self):
        self.existing = tempfile.mktemp()
        self.new = tempfile.mktemp()

        # create some data
        n = 10000
        self.nrows = n
        lvl1 = np.random.choice(["some", "really", "random", "stuff", "not", "very", "creative"], size=n)
        lvl2 = np.random.choice(["well", "some", "more", "levels", "could", "be", "nice"], size=n)
        pids = list(map(lambda x: ".".join(x), zip(lvl1, lvl2)))
        df = pd.DataFrame(dict(date=pd.date_range('20130101', periods=10000, freq="H"),
                               page_id=pids,
                               id=np.random.choice(np.arange(10), size=10000)))
        self.data = pd.get_dummies(df, columns=["page_id"])

        self.data.to_hdf(self.existing, key="/df")

    def test_exists(self):
        target = Hdf5TableTarget(self.existing, key="df")
        self.assertTrue(target.exists())

        target = Hdf5TableTarget(self.new, key="df")
        self.assertFalse(target.exists())

        with target.open("w") as tb:
            tb.write(self.data)
        self.assertTrue(target.exists())

    def test_exists_splitted(self):
        target = Hdf5TableTarget(self.new, key="df", split=2)
        self.assertFalse(target.exists())

        with target.open("w") as tb:
            tb.write(self.data)
        self.assertTrue(target.exists())

    def test_read(self):
        target = Hdf5TableTarget(self.existing, key="df")
        df = target.open().read(autoclose=1)
        pdt.assert_frame_equal(df, self.data)

    def test_write(self):
        target = Hdf5TableTarget(self.new, key="df")
        with target.open("w") as tb:
            tb.write(self.data)
        df = target.open().read(autoclose=1)
        pdt.assert_frame_equal(df, self.data)

    def test_atomicity(self):
        target = Hdf5TableTarget(self.new, key="df")
        try:
            with target.open("w") as tb:
                tb.write(self.data.iloc[:100])
                raise ValueError()
        except ValueError:
            self.assertFalse(target.exists())

    def test_write_key_existent(self):
        def temp_func():
            target = Hdf5TableTarget(self.existing, key="df")
            with target.open("w") as tb:
                tb.write(self.data)

        self.assertRaises(ValueError, temp_func)

    def tearDown(self):
        if os.path.exists(self.existing):
            os.remove(self.existing)
        if os.path.exists(self.new):
            os.remove(self.new)


@attr('hdf5')
class Hdf5TableTest(unittest.TestCase):
    def setUp(self):
        # create empty hdf5 file
        self.store = tempfile.mktemp()

        # create some data
        n = 10000
        self.nrows = n
        lvl1 = np.random.choice(["some", "really", "random", "stuff", "not", "very", "creative"], size=n)
        lvl2 = np.random.choice(["well", "some", "more", "levels", "could", "be", "nice"], size=n)
        pids = list(map(lambda x: ".".join(x), zip(lvl1, lvl2)))
        df = pd.DataFrame(dict(date=pd.date_range('20130101', periods=10000, freq="H"),
                               page_id=pids,
                               id=np.random.choice(np.arange(10), size=10000)))
        self.data = pd.get_dummies(df, columns=["page_id"], prefix="", prefix_sep="")

    def test_single_io(self):
        # simple write
        with AtomicHdf5Table(self.store, key="/df", mode="w", index_cols=("id",)) as table:
            table.write(self.data)

        # simple read
        with AtomicHdf5Table(self.store, key="/df", mode="r", index_cols=("id",)) as table:
            df = table.read()
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

        # test_columns_sorted
        self.assertTrue(cols_sorted(df))

    def test_single_chunked_io(self):
        # appending write
        with AtomicHdf5Table(self.store, key="/df", mode="a", index_cols=("id",)) as table:
            for i in range(0, self.data.shape[0], 1000):
                chunk = self.data.iloc[i:i + 1000]
                table.write(chunk)
            df = table.read()
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

        # chunked read
        with AtomicHdf5Table(self.store, key="/df", mode="r", index_cols=("id",)) as table:
            frames = []
            for chunk in table.read(chunksize=1000):
                frames.append(chunk)
                self.assertTrue(cols_sorted(chunk))
            df = pd.concat(frames, axis=0)
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

        # read with start, stop
        with AtomicHdf5Table(self.store, key="/df", mode="r", index_cols=("id",)) as table:
            df = table.read(start=0, stop=1)
        self.assertEqual(df.shape[0], 1)
        self.assertTrue(cols_sorted(df))

    def test_splitted_io(self):
        # simple write
        with AtomicHdf5Table(self.store, key="/df", mode="w", index_cols=("id",), split=4) as table:
            table.write(self.data)

        with AtomicHdf5Table(self.store, key="/df", mode="r", index_cols=("id",), split=4) as table:
            self.assertTrue("/df/index_table" in table.fs)
            self.assertTrue("/df/subtable_1" in table.fs)
            self.assertTrue("/df/subtable_2" in table.fs)
            self.assertTrue("/df/subtable_3" in table.fs)

        # simple read
        with AtomicHdf5Table(self.store, key="/df", mode="r", index_cols=("id",), split=4) as table:
            df = table.read()
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

        # read with start, stop
        with AtomicHdf5Table(self.store, key="/df", mode="r", index_cols=("id",), split=4) as table:
            df = table.read(start=0, stop=1)
        self.assertEqual(df.shape[0], 1)
        self.assertTrue(cols_sorted(df))

    def test_manual_writing_splitted(self):
        data_splits = [self.data[c_split] for c_split in np.array_split(self.data.columns, 4)]

        # write table with sub_key
        with AtomicHdf5Table(self.store, key="/df", mode="a") as table:
            for i, char in enumerate(list("WHAT")):
                table.write(data_splits[i], sub_key="{}_table".format(char))

        # full read
        with AtomicHdf5Table(self.store, key="/df", mode="r") as table:
            df = table.read()
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

        # chunked read
        with AtomicHdf5Table(self.store, key="/df", mode="r") as table:
            frames = []
            for chunk in table.read(chunksize=1000):
                frames.append(chunk)
            df = pd.concat(frames, axis=0)
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

    def test_splitted_chunked_io(self):
        # appending write
        with AtomicHdf5Table(self.store, key="/df", mode="a", index_cols=("id",), split=4) as table:
            for i in range(0, self.data.shape[0], 1000):
                chunk = self.data.iloc[i:i + 1000]
                table.write(chunk)

        with AtomicHdf5Table(self.store, key="/df", mode="r") as table:
            df = table.read()
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

        # chunked read
        with AtomicHdf5Table(self.store, key="/df", mode="r") as table:
            frames = []
            for chunk in table.read(chunksize=1000):
                frames.append(chunk)
                self.assertTrue(cols_sorted(chunk))
            df = pd.concat(frames, axis=0)
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))

    def test_multi_key_io(self):
        keys = ["table_{}".format(i) for i in range(10)]
        # write multiple tables in same storage
        for idx, key in enumerate(keys):
            chunk = self.data.iloc[idx * 1000:(idx + 1) * 1000]
            with AtomicHdf5Table(self.store, key=key, mode="a", index_cols=("id",)) as table:
                table.write(chunk)
        # simple read
        with AtomicHdf5Table(self.store, key=None, mode="r", index_cols=("id",)) as table:
            df = table.read(keys=keys)
        pdt.assert_frame_equal(df, self.data.sort_index(axis=1))
        # read with select expr
        with AtomicHdf5Table(self.store, key=None, mode="r", index_cols=("id",)) as table:
            df = table.read(keys=keys, where="id >= 0 & id <=5")
        sorted_df = self.data.sort_index(axis=1)
        pdt.assert_frame_equal(df.reset_index(drop=1),
                               sorted_df[(sorted_df.id >= 0) & (sorted_df.id <= 5)].reset_index(drop=1))

    def test_attributes(self):

        with AtomicHdf5Table(self.store, key="/df", mode="w", index_cols=("id",)) as table:
            table.write(self.data)
            self.assertEqual(table.nrows, self.nrows)
        with AtomicHdf5Table(self.store, key="/df", mode="r", index_cols=("id",)) as table:
            self.assertEqual(table.fn, self.store)

    def test_forbidden_params(self):
        pass

    def test_atomicity(self):
        try:
            with AtomicHdf5Table(self.store, key="/df", mode="a", index_cols=("id",)) as table:
                for i in range(0, self.data.shape[0], 1000):
                    chunk = self.data.iloc[i:i + 1000]
                    table.write(chunk)
                    if i == 4 * 1000:
                        raise ValueError()
        except ValueError:
            with AtomicHdf5Table(self.store, key=None, mode="r", index_cols=("id",)) as table:
                self.assertFalse("/df" in table.fs)

    def tearDown(self):
        if os.path.exists(self.store):
            os.remove(self.store)
