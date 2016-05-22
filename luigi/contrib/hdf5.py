import os
import luigi
import logging
import time
import sys
import tempfile

logger = logging.getLogger('luigi-interface')

try:
    import pandas as pd
except ImportError:
    logger.warning("Loading hdf5 module without the python packages pandas. \
        This will crash at runtime if pandas functionality is used.")

    class Object(): # ensures docs are built will lead to fail at runtime
        HDFStore = object
    pd = Object()

if sys.version_info < (3, 3):
    FileExistsError = OSError


class Hdf5FileSystem(luigi.target.FileSystem):
    """
        Simple class to represent a Filesystem inside a hdf5 storage
    """

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
    """
    A target that writes to a temporary node and then moves it to it's final destination. All targets are safe to use
    for multithreading. As pytables does not allow simultaneous access to a storage each operation is protected by
    locking the resource and unlocking it after finishing using file locks.
    Other targets requesting access will wait until it is freed.

    :note:
            When table is appended there is no atomicity of write operation. If you want atomic write operation consider
            using a fixed storage or set append to False.
    """

    def __init__(self, hdf5_file, key, index_cols=(),
                 append=True, expected_rows=None, format="table",
                 complib="blosc"):
        """

        :param hdf5_file: path to hdf5file will be created if not existent.
        :param key: key or node inside the storage to write to.
        :param index_cols: this columns will be fully indexed for faster searches.
                           Consider indexing often queried columns.
        :param append:  if True the created table will be appendable. Default is True.
        :param expected_rows: for better I/O a good estimate of the table size should be estimated.
        :param format: either 'fixed' or 'table'
        :param complib: which compression library to use. Complevel is default set to 1 as results
                        do not differ to much using higher compression
        :return: A target object
        """
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
        """
        Opens the target and returns the whole table as a pd.DataFrame

        :param kwargs: optional kwargs will be passed to pytables
        :return: a pd:DataFrame containing the targets data
        """
        with SafeHDFStore(self.store, "r") as store:
            return store.get(self.key, **kwargs)

    def read_chunks(self, chunksize=1e5, expr_list=()):
        """
        Allows to read a very big storage in chunks. This can be done either by chunking the complete storage.
        Or by a list of Expression. This is very useful if you want to aggregate your data as it allows for splitting
        this operation in chunks. For example consider:

        .. code-block:: python

            class AggregateDailyTask(luigi.Task)

            ...

            def run():
                 user_activites = self.input()
                 for df in user_activites.read_chunks(
                           expr_list=[" user_id = {}".format(id) for id in users]):
                     self.output().write(df.groupby("date").sum())

        This Allows for easily selecting only the interesting data from your store or splitting data for parallel processing

        :param chunksize: specifies how many rows will be returned per chunk.
        :param expr_list: if not empty chunks will be selected by evaluating each expression.
        :return: an iterator returning the rows in a pandas.DataFrame
        """
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
        """
        Writes or appends dataframe to specified node in storage. If the storage does not exist it will be created.
        If the node in the storage already exists and is appendable the content of df will be appended.

        :note:
            If you append to the table the column schema of the dataframe must match the already stored data.

        :param df: pd.DataFrame
        :return: None
        """
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

    def _update_store(self, df, store):
        """
        Writes/appends or creates the store and indexes certain columns if specified.

        :param df: pd.DataFrame containing data to write
        :param store: open SafeHDFSStore object
        :return:
        """
        if self.key not in store:
            store.put(self.tmp, df, format=self.format, append=self.append,
                      data_columns=self.index_cols, encoding="utf-8", expectedrows=self.expected_rows)
            if len(self.index_cols) > 0:
                store.create_table_index(self.key, columns=self.index_cols,
                                         optlevel=9, kind="full")
        else:
            store.append(self.key, df)


class Hdf5RowTarget(Hdf5TableTarget):
    """
    A target that allows specifying certain rows using a pd.Term string

    :note:
        if row expression returns anything the target will be regarded as existent.
        So be smart about how you define it.
    """

    def __init__(self, hdf5_file, key, row_expr=None, index_cols=(),
                 append=True, expected_rows=None, format="table"):
        """
        :param row_expr: The expression to identify the existence of this target e.g. "entry_id = 3".
                         The columns used by the expression must be indexed for this to work.
                         See `pandas documentation <http://pandas.pydata.org/pandas-docs/stable/enhancingperf.html
                         #expression-evaluation-via-eval-experimental>`_
                         for more information.
        :type row_expr: pd.Term, str
        :return: A target object pointing to the rows specified via the row_expr
        """
        super(Hdf5RowTarget, self).__init__(hdf5_file, key, index_cols,
                                            append, expected_rows, format)
        self.row_expr = row_expr
        if isinstance(row_expr, str):
            self.row_expr = pd.Term(row_expr)

    def write(self, df):
        """
        Same as :py:meth:`Hdf5TableTarget.close`

        :param df:
        :return:
        """
        super(Hdf5RowTarget, self).write(df)

    def open(self, **kwargs):
        """
        Returns the data selected by row_expr as pd.DataFrame.

        :param kwargs: kwargs passed to pytables. The where argument will be replaced with row_expr if not None.
        :return: A pd.DataFrame containing the data the expression points to
        """
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


class SafeHDFStore(pd.HDFStore):
    """
    Modified Piettro Battison's solution on handling safe multiprocessing HDFStore access. This solution currently creates
    a file lock as it is hard to pass a shared Lock object to luigi workers. You can use this with a context manager.

    .. code-block:: python

        with SafeHDFStore("some_storage.h5", "w") as store:
            store.put("/some_node", df)
    """

    def __init__(self, *args, **kwargs):
        probe_interval = kwargs.pop("probe_interval", 1)
        self._lock = "%s.lock" % args[0]
        while True:
            try:
                self._flock = os.open(self._lock, os.O_CREAT |
                                      os.O_EXCL |
                                      os.O_WRONLY)
                break
            except FileExistsError:
                time.sleep(probe_interval)
        try:
            pd.HDFStore.__init__(self, *args, **kwargs)
        except Exception as e:
            os.close(self._flock)
            os.remove(self._lock)
            raise e

    def __exit__(self, *args, **kwargs):
        self.close()

    def close(self):
        """
        Closes the store and realeses the file lock
        :return:
        """
        pd.HDFStore.close(self)
        os.close(self._flock)
        os.remove(self._lock)
