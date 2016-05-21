import os
import pandas as pd
import logging
import luigi
import tempfile
from drtools.utils.synchron import SafeHDFStore


class Hdf5FileSystem(luigi.target.FileSystem):
    def __init__(self, hdf5_file):
        self.store = hdf5_file

    def remove(self, path, recursive=True, skip_trash=True):
        with SafeHDFStore(self.store, mode="a") as store:
            store.remove(path)

    def move(self, path, dest):
        with SafeHDFStore(self.store, mode="a") as store:
            table = store._handle  # get pytables handle
            os.path.basename(dest)
            table.move_node(path, newparent=os.path.dirname(dest),
                            newname=os.path.basename(dest), createparents=True)

    def exists(self, path):
        try:
            if os.path.exists(self.store):
                with SafeHDFStore(self.store, mode="r") as store:
                    store.get(path)
                    return True
            else:
                return False
        except (TypeError, OSError, KeyError):
            return False


class Hdf5TableTarget(luigi.target.FileSystemTarget):

    def __init__(self, hdf5_file, key, index_cols=(),
                 append=True, expected_rows=None, format="table",
                 complib="blosc"):
        super(Hdf5TableTarget, self).__init__(key)
        self.store = hdf5_file
        self.path = key  # for compatibility with other file-like targets
        self.key = key
        self.append = append if format == "table" else False
        self.expected_rows = expected_rows
        self.format = format
        self.index_cols = index_cols
        self.comp = complib
        self.filesystem = Hdf5FileSystem(hdf5_file)
        self.tmp = tempfile.mkdtemp()

    @property
    def fs(self):
        return self.filesystem

    def open(self, **kwargs):
        with SafeHDFStore(self.store, "r") as store:
            return store.get(self.key)

    def read_chunks(self, chunksize=1e5, expr_list=()):
        chunksize = int(chunksize)
        store = SafeHDFStore(self.store, "r")
        nrows = store.get_storer(self.key).nrows
        is_table = store.get_storer(self.key).is_table
        store.close()
        if is_table:
            if len(expr_list) == 0:
                for i in range(0, nrows, chunksize):
                    store = SafeHDFStore(self.store, "r")
                    df = store.select(self.key, start=i, stop=i + chunksize)
                    store.close()
                    yield df
            else:
                for expr in expr_list:
                    store = SafeHDFStore(self.store, "r")
                    df = store.select(self.key, where=expr)
                    store.close()
                    yield df
        else:
            raise ValueError("This storage does not support partial reading!")

    def write(self, df):
        try:
            if os.path.exists(self.store):
                with SafeHDFStore(self.store, mode="r+") as store:
                    self._update_store(df, store)
            else:
                with SafeHDFStore(self.store, mode="w", complevel=1, complib=self.comp) as store:
                    self._update_store(df, store)
                self.move_to_final_destination()
        except Exception as e:
            # delete everything if anything went wrong
            # not 100% failsafe but better than nothing
            if os.path.exists(self.store):
                with SafeHDFStore(self.store) as store:
                    store.remove(key=self.tmp)
            raise e

    def move_to_final_destination(self):
        self.fs.move(self.tmp, self.key)

    def _update_store(self, df, store, overwrite=False):
        if self.key not in store:
            store.put(self.tmp, df, format=self.format, append=self.append,
                      data_columns=self.index_cols, encoding="utf-8", expectedrows=self.expected_rows)
            if len(self.index_cols) > 0:
                store.create_table_index(self.key, columns=self.index_cols,
                                         optlevel=9, kind="full")
        else:
            store.append(self.key, df)


class Hdf5RowTarget(Hdf5TableTarget):
    def __init__(self, hdf5_file, key, row_expr=None, index_cols=(),
                 append=True, expected_rows=None, format="table"):
        super(Hdf5RowTarget, self).__init__(hdf5_file, key, index_cols,
                                            append, expected_rows, format)
        self.row_expr = row_expr
        if isinstance(row_expr, str):
            self.row_expr = pd.Term(row_expr)

    def write(self, df):
        super(Hdf5RowTarget, self).write(df)

    def open(self, **kwargs):
        with SafeHDFStore(self.store) as store:
            if self.row_expr:
                kwargs["where"] = self.row_expr
            return store.select(self.key, **kwargs)

    def exists(self):
        if not os.path.exists(self.store):
            return False
        try:
            with SafeHDFStore(self.store, mode="r") as store:
                if self.row_expr is not None:
                    res = store.select_as_coordinates(self.key, where=self.row_expr)
                    if res.shape[0] > 0:
                        return True
                    else:
                        return False
                else:
                    if store.get_storer(self.key) is not None:
                        return True
                    else:
                        return False
        except (OSError, KeyError, AttributeError):
            return False
