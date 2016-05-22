import tempfile
import shutil
import numpy as np
import pandas.util.testing as pdt
import pandas as pd
import unittest
from datetime import datetime

from luigi.contrib.hdf5 import *


class Hdf5TargetTest(unittest.TestCase):
    def setUp(self):
        n = 10000
        lvl1 = np.random.choice(["some", "really", "random", "stuff", "not", "very", "creative"], size=n)
        lvl2 = np.random.choice(["well", "some", "more", "levels", "could", "be", "nice"], size=n)
        pids = list(map(lambda x: ".".join(x), zip(lvl1, lvl2)))
        self.df = pd.DataFrame(dict(date=pd.date_range('20130101', periods=10000, freq="H"),
                                    page_id=pids,
                                    id=np.random.choice(np.arange(10), size=10000)))
        self.dir = tempfile.mkdtemp()
        self.hdf5_file_empty = os.path.join(self.dir, "hdf5-luigi-target-test-write.h5")

        self.hdf5_file = os.path.join(self.dir, "hdf5-luigi-target-test-read.h5")
        with SafeHDFStore(self.hdf5_file, "w") as store:
            store.put("/df", self.df, complevel=1, complib="blosc", data_columns=True, format="table", append=True)

    def testTableTargetExists(self):
        correct_true = Hdf5TableTarget(self.hdf5_file, "/df").exists()
        self.assertTrue(correct_true, "Target exists method returned false for existing target")

        correct_false = Hdf5TableTarget(self.hdf5_file_empty, "/df").exists()
        self.assertFalse(correct_false, "Non existing file but claimed target exists")

        correct_false = Hdf5TableTarget(self.hdf5_file, "/random").exists()
        self.assertFalse(correct_false, "Non existing group but claimed target exists")

    def testRowTargetExists(self):
        print("Runinng target test on existing rows")
        correct_true = Hdf5RowTarget(self.hdf5_file, "/df",
                                     row_expr=pd.Term("(date > 20130101) & (date < 20140101)")).exists()
        self.assertTrue(correct_true, "Got existent rows but claimed to be non existent")

        print("Checking if existing group is recognized")
        correct_true = Hdf5RowTarget(self.hdf5_file, "/df").exists()
        self.assertTrue(correct_true, "Got no row_expr and target exists, but claims to be non existent")

        print("Checking if missing file returns False")
        correct_false = Hdf5RowTarget(self.hdf5_file_empty, "/df").exists()
        self.assertFalse(correct_false)
        #
        print("Checking if non existent group returns missing")
        correct_false = Hdf5RowTarget(self.hdf5_file, "/test").exists()
        self.assertFalse(correct_false)

        print("Checking if non existing rows return False")
        correct_false = Hdf5RowTarget(self.hdf5_file, "/df", row_expr=pd.Term("date > 20410101")).exists()
        self.assertFalse(correct_false)

    def testTableRead(self):
        target = Hdf5TableTarget(self.hdf5_file, "/df")
        dfa = target.open()
        pdt.assert_frame_equal(dfa, self.df)

    def testRowRead(self):
        target = Hdf5RowTarget(self.hdf5_file, "/df", row_expr=pd.Term("date > 20130101 & date < 20140101"))
        dfa = target.open()
        pdt.assert_frame_equal(dfa,
                               self.df[(self.df.date > datetime(2013, 1, 1)) & (self.df.date < datetime(2014, 1, 1))])

    def testWrite(self):
        target = Hdf5TableTarget(self.hdf5_file_empty, "/df")
        target.write(self.df)
        dfa = target.open()
        pdt.assert_frame_equal(dfa, self.df)

    def testDateExpression(self):
        expr = "date >= {:%Y%m%d} & date <= {:%Y%m%d}".format(*(datetime(2013, 1, 1), datetime(2013, 5, 1)))
        target = Hdf5RowTarget(self.hdf5_file, key="/df", row_expr=expr)
        dfa = target.open()
        df = self.df.query(expr)
        self.assertTrue(target.exists, "Did not evaluate expression correctly:\n{}".format(expr))
        pdt.assert_frame_equal(dfa, df)

    def testSelectUserSubset(self):
        expr = "id = [{}]".format(",".join(np.arange(100).astype(str)))
        target = Hdf5RowTarget(self.hdf5_file, key="/df")
        dfa = target.open(where=pd.Term(expr))
        df = self.df[self.df.id.isin(np.arange(100))]
        self.assertTrue(target.exists, "Did not evaluate expression correctly:\n{}".format(expr))
        pdt.assert_frame_equal(dfa, df)

    def testWriteMultiIndex(self):
        target = Hdf5RowTarget(self.hdf5_file_empty, "/df", index_cols=("id", "date"))
        target.write(self.df)
        dfa = target.open(where=pd.Term("id > 0 & date >= 20130101"))
        df = self.df[(self.df.id > 0) & (self.df.date >= "20130101")]
        pdt.assert_frame_equal(dfa, df)

    def testWriteSingleIndex(self):
        target = Hdf5RowTarget(self.hdf5_file_empty, "/df", index_cols=("id",))
        target.write(self.df)
        dfa = target.open(where=pd.Term("id > 0"))
        df = self.df[self.df.id > 0]
        dfa.sort_values("id", inplace=True)
        df.sort_values("id", inplace=True)
        # print(set(dfa.id.unique()), set(df.id.unique()) )
        pdt.assert_frame_equal(dfa, df)

    def testAppend(self):
        target = Hdf5TableTarget(self.hdf5_file_empty, "/df")
        target.write(self.df.iloc[:100])
        target.write(self.df.iloc[100:])
        dfa = target.open()
        pdt.assert_frame_equal(dfa, self.df)

    def testChunkRead(self):
        parts = []
        target = Hdf5TableTarget(self.hdf5_file, "/df")
        for df in target.read_chunks(1000):
            parts.append(df)
        dfa = pd.concat(parts)
        pdt.assert_frame_equal(dfa, self.df)

    def testChunkedExprReading(self):
        parts = []
        target = Hdf5TableTarget(self.hdf5_file, "/df")
        for df in target.read_chunks(expr_list=["id = {}".format(i) for i in range(10)]):
            parts.append(df)
        dfa = pd.concat(parts)
        dfa.sort_values(["id","date"], inplace=True)
        df = self.df.sort_values(["id", "date"])
        pdt.assert_frame_equal(dfa, df)

    def testMixedTypeColumn(self):
        target = Hdf5TableTarget(self.hdf5_file_empty, "/df")
        self.df.loc[0, "id"] = True
        self.df.loc[1, "id"] = "somestring"
        with self.assertRaises(TypeError):
            target.write(self.df.iloc[:100])

    def tearDown(self):
        shutil.rmtree(self.dir)


if __name__ == "__main__":
    unittest.main()
