import io
import os
import sys
import logging
import tempfile

import luigi

LOGGER = logging.getLogger("luigi-interface")

try:
    import pandas as pd
except ImportError:

    class Object:
        HDFStore = object

    pd = Object()
    LOGGER.warning("Loading hdf5 module without the python packages pandas. \
        This will crash at runtime if pandas functionality is used.")
try:
    import numpy as np
    from tables import NoSuchNodeError
except ImportError:
    LOGGER.warning("Loading hdf5 module without the python packages tables. \
            This will crash at runtime if tables functionality is used.")

if sys.version_info < (3, 3):
    FileExistsError = OSError


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

    def __init__(self, store, key, append=True, index_cols=(), expected_rows=None, format="table", split=None,
                 complib="blosc", selector="index_table"):
        """

        :param store: path to hdf5file will be created if not existent.
        :param key: key or node inside the storage to write to.
        :param index_cols: this columns will be fully indexed for faster searches.
                           Consider indexing often queried columns.
        :param append:  if True the created table will be appendable. Only usable if table is openened in append mode.
                        Multiple class to write will then append to the table. Default is True.
        :param expected_rows: for faster I/O a good estimate of the table size should be estimated.
        :param format:  either 'fixed' or 'table'
        :param complib: which compression library to use. Complevel is default set to 1 as results
                        do not differ to much using higher compression
        :param split:  A dictionary or an integer useful to split tables along their columns. If a dictionary is passed
                       it should describe a mapping between tables and columns. A table with the passed selector should
                       be present in the keys. All indexed columns should be mapped to the selector table. Passing an
                       integer will automatically create a valid split dictionary, where the number specifies the number
                       of desired tables.
        :param format: Only useful if split is not None. Specifies the selector table. This key must be present in the
                       split dictionary.
        :return: A target object
        """
        super(Hdf5TableTarget, self).__init__(key)
        self.path = key
        self.filesystem = Hdf5FileSystem(store)
        self.table_params = dict(
            store=store,
            key=key,
            append=append,
            index_cols=index_cols,
            expected_rows=expected_rows,
            format=format,
            split=split,
            complib=complib,
            selector=selector
        )
        # check if parameters are ok
        AtomicHdf5Table.check_integrity(**self.table_params)

    @property
    def fs(self):
        return self.filesystem

    @property
    def fn(self):
        return self.table_params["store"]

    def open(self, mode="r", **kwargs):
        return AtomicHdf5Table(mode=mode, **self.table_params)


class Hdf5StoreTarget(Hdf5TableTarget):
    """
    Refers to a store without any particular tables
    """

    def __init__(self, *args, **kwargs):
        super(Hdf5StoreTarget, self).__init__(*args, **kwargs)
        self.table_params["key"] = None

    def exists(self):
        return os.path.exists(self.table_params["store"])

    def open(self, mode="r", **kwargs):
        if mode != "r":
            raise ValueError("Hdf5StoreTarget is not writeable. Use Hdf5TableTarget instead")
        return super(Hdf5StoreTarget, self).open(mode="r", **kwargs)


def multi_target_read(target_list, concat_axis=0, **kwargs):
    frames = []
    for t in target_list:
        with t.open("r") as table:
            frames.append(table.read(**kwargs))
    return pd.concat(frames, axis=concat_axis).sort_index(axis=1)


class Hdf5FileSystem(luigi.target.FileSystem):
    """
        Simple class to represent a Filesystem inside a hdf5 storage
    """

    def __init__(self, hdf5_file):
        self.store = hdf5_file

    def remove(self, path, recursive=True, skip_trash=True):
        with pd.HDFStore(self.store, mode="a") as store:
            store.remove(path)

    def move(self, path, dest):
        with pd.HDFStore(self.store, mode="a") as store:
            table = store._handle  # get pytables handle
            table.move_node(path, newparent=os.path.dirname(dest),
                            newname=os.path.basename(dest), createparents=True)

    def exists(self, path, store=None):
        try:
            if os.path.exists(self.store) and store is None:
                with pd.HDFStore(self.store, mode="r") as store:
                    return path in store
            elif store is not None:
                return path in store
            else:
                return False
        except (TypeError, OSError, KeyError):
            return False

    def get_table(self, path):
        pass

    def listdir(self, path, store=None):
        depth = len(path.split("/"))
        children = []
        need_close_store = False
        if store is None:
            need_close_store = True
            store = pd.HDFStore(self.store, mode="r")
        for child in [k for k in store.keys() if k.startswith((path, "/" + path))]:
            children.append("/".join(child.split("/")[:depth + 1]))
        if need_close_store:
            store.close()
        return children

    def __contains__(self, item):
        with pd.HDFStore(self.store, mode="r") as store:
            return item in store


class Hdf5Table(object):
    def __init__(self, store, key, mode="r", append=True, index_cols=(),
                 expected_rows=None, format="table", split=None,
                 complib="blosc", selector="index_table"):
        """
        Internal class to handle reads and writes to tables inside hdf5 stores.
        """
        Hdf5Table.check_integrity(store, key, mode=mode, split=split, append=append, index_cols=index_cols,
                                  expected_rows=expected_rows, format=format, complib=complib, selector=selector)
        # initialize attributes from parameters
        self.store = store
        self.key = key if key is not None and key.startswith("/") else "/{}".format(key)
        self.mode = mode
        self.append = False if format != "table" else append
        self.index_cols = index_cols
        self.expected_rows = expected_rows
        self.format = format
        self.complib = complib
        self.selector = selector
        if not isinstance(split, dict):
            self.split = 1 if split is None else split

        # some internal attributes
        self.closed = False
        self.is_table = None
        self.filesystem = Hdf5FileSystem(store)
        self.written = False  # used to see if we have to perform the final move operation
        self.first_get_subtable_call = True
        self._is_open = True

    def __enter__(self, *args, **kwargs):
        return self

    @staticmethod
    def check_integrity(store, key, mode="r", split=None, **kwargs):
        """
        Checks if parameters are valid else raises ValueError
        """
        if not isinstance(store, str):
            raise ValueError("Expected path to storage as string")

        if not isinstance(key, str) and key is not None:
            raise ValueError("Expected key in storage as string")

        if key is None and mode != "r":
            raise ValueError("Cannot initialize a table without key in write or append mode")

        if mode not in "war":
            raise ValueError("Unsupported file mode")

        if isinstance(split, dict):
            if "index_table" not in split.keys():
                raise ValueError("Split dictionary has no main key which specifies the main indexing table")

    @property
    def fs(self):
        return self.filesystem

    @property
    def fn(self):
        return self.store

    @property
    def nrows(self):
        """
        :return: The tables row count
        """
        if self.key is None:
            return None
        if self.split > 1:
            key = os.path.join(self.key, "index_table")
        else:
            key = self.key
        with pd.HDFStore(self.store, "r") as store:
            return store.get_storer(key).nrows

    def write(self, df, sub_key=None, **kwargs):
        """
        Saves the passed :class: pandas.DataFrame to the specified hdf5 storage. If table was openened in append mode
        and append was set to true multiple calls to write will append to the table.

        :param df: A Dataframe object to be saved into the store
        :param sub_key: Useful to write a splitted table into the parent key. Note that all sub-tables have to be of
                        the same number rows and will be indexed using the first written table
        :return:
        """
        if df.empty:
            raise ValueError("Received empty DataFrame")
        if self.key is None and sub_key is None:
            raise ValueError("No key was set")
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if self.mode == "r":
            raise io.UnsupportedOperation("not writable")
        with pd.HDFStore(self.store, mode=self.mode) as store:
            self._update_store(df.sort_index(axis=1), store, subtable=sub_key, **kwargs)
        self.written = True

    def read(self, keys=None, concat_axis=0, chunksize=None, autoclose=False, **kwargs):
        """
        Performs a read operation on a table in a hdf5 store

        :param keys: Allows for selecting multiple keys
        :param concat_axis: If multiple keys are selected along which axis to concatenate the results
        :param chunksize: If passed this method will return an iterator over row chunks
        :param autoclose: If True the store will be closed after finishing the read operation
        :param kwargs: Further keyword arguments will be passed to pytables select method
        :return: :class: pandas.DataFrame
        """
        # settings.logger.debug("Reading")
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if chunksize is None:
            res = self._read(keys=keys, concat_axis=concat_axis, **kwargs).sort_index(axis=1)
        else:
            res = self._read_chunked(chunksize)

            # Monkey patch the :class:~pandas.io.pytables.TableIterator to
            # return the columns in an ordered fashion
            return sorted_table_iterator(res)
        if autoclose:
            self.close(False)
        return res

    def close(self, exc=False):
        """
        Closes the file and creates an index if any index columns where specified

        :param exc: parameter used to indicate that an exception happened before an IO operation finished if
                    True no index will be created
        :return:
        """
        if self.mode != "r" and not exc:
            if len(self.index_cols) > 0:
                with pd.HDFStore(self.store, mode="a") as store:
                    if self.split <= 1:
                        self._create_csi(store)
                    else:
                        for key in self.fs.listdir(self.key, store):
                            self._create_csi(store, key=key)
        self._is_open = False

    def __exit__(self, exc_type, exc_value, traceback):
        exc = True if exc_type is not None else False
        self.close(exc)

    def _read(self, keys=None, concat_axis=0, **kwargs):
        """reads a table from a storage supports the keys argument to select from multiple tables"""
        if self.key is None and keys is None:
            raise ValueError("Store was initialized without key, so keys argument must be provided")
        if keys is not None and self.split > 1:
            raise NotImplementedError("reading from multiple splitted tables not implemented yet")
        if keys is not None:
            # settings.logger.debug("KEYS OPEN")
            frames = []
            ignore_idx = True if concat_axis == 0 else False
            with pd.HDFStore(self.store, mode="r") as store:
                for idx, k in enumerate(keys):
                    frames.append(store.select(k, **kwargs))
            return pd.concat(frames, axis=concat_axis, ignore_index=ignore_idx)

        if self._key_is_table:
            # settings.logger.debug("NORMAL OPEN")
            with pd.HDFStore(self.store, mode="r") as store:
                data = store.select(self.key, **kwargs)
            return data
        else:
            # pandas bug does not allow start stop in select as multiple
            keys = self.filesystem.listdir(self.key)
            # with pd.HDFStore(self.store, mode="r") as store:
            #     return store.select_as_multiple(keys, **kwargs)
            frames = []
            with pd.HDFStore(self.store, mode="r") as store:
                for k in keys:
                    frames.append(store.select(k, **kwargs))
            return pd.concat(frames, axis=1)

    def _read_chunked(self, chunksize):
        is_table = self._key_is_table
        store = pd.HDFStore(self.store, "r")
        try:
            if is_table:
                return store.select(key=self.key, chunksize=chunksize, auto_close=1)
            else:
                keys = self.filesystem.listdir(self.key, store)
                return store.select_as_multiple(keys, chunksize=chunksize, auto_close=1)
        except:
            store.close()
            raise

    @property
    def _key_is_table(self):
        if self.is_table is None:
            with pd.HDFStore(self.store, mode="r") as store:
                try:
                    self.is_table = store.get_storer(self.key).is_table
                except TypeError:
                    self.is_table = False
        return self.is_table

    def _create_splitted_tables(self, df, store, **kwargs):
        # create multiple tables if split is defined
        for key, column_split in self._split_dict(df.columns).items():
            # key = os.path.join(self.key, os.path.basename(subtable_key))
            if all([i in column_split for i in self.index_cols]):
                icols = self.index_cols
            else:
                icols = None
            store.put(key, df[column_split], format=self.format, append=self.append,
                      data_columns=icols, encoding="utf-8", expectedrows=self.expected_rows,
                      complib=self.complib, **kwargs)

    def _update_store(self, df, store, subtable=None, **kwargs):
        """
        Writes/appends or creates the store.

        :param df: pd.DataFrame containing data to write
        :param store: open SafeHDFSStore object
        :return:
        """
        if subtable is not None:
            sub_key = self._get_subtable_key(subtable)

            if subtable is not None and not self.fs.exists(sub_key, store):
                store.put(sub_key, df, format=self.format, append=self.append,
                          data_columns=self.index_cols, encoding="utf-8", expectedrows=self.expected_rows,
                          complib=self.complib, **kwargs)

            elif subtable is not None and self.fs.exists(sub_key, store):
                store.append(sub_key, df, **kwargs)

        elif self.key not in store:
            if self.split <= 1:
                store.put(self.key, df, format=self.format, append=self.append,
                          data_columns=self.index_cols, encoding="utf-8", expectedrows=self.expected_rows,
                          complib=self.complib, **kwargs)
            else:
                self._create_splitted_tables(df, store, **kwargs)
        else:
            try:
                if self.split <= 1:
                    store.append(self.key, df, **kwargs)
                else:
                    self._check_key_integrity(self._split_dict(df.columns).keys(), store)
                    store.append_to_multiple(self._split_dict(df.columns), df,
                                             selector=os.path.join(self.key, self.selector),
                                             data_columns=self.index_cols, **kwargs)
            except ValueError:
                # if self.split <= 1:
                #     temp = store.select(self.key, start=0, stop=1)
                #     # debug.compare_columns(temp, df, LOGGER.error)
                # else:
                #     for key in self.fs.listdir(self.key):
                #         LOGGER.error("Error appending to multiple. Comparison of columns for {}".format(key))
                #         temp = store.select(key, start=0, stop=1)
                #         # debug.compare_columns(temp, df, LOGGER.error)
                raise

    def _get_subtable_key(self, subtable):
        key = os.path.join(self.key, "subtable_{}".format(subtable))
        if isinstance(subtable, str):
            key = os.path.join(self.key, subtable)

        if self.first_get_subtable_call:
            self.first_get_subtable_call = False
            key = os.path.join(self.key, self.selector)

        self.split += 1
        return key

    def _create_csi(self, store, key=None, reindex=False):
        """
        Creates a fully sorted index
        :return:
        """
        key = self.key if key is None else key
        table = store.get_storer(key)
        if not table.is_table:
            raise ValueError("Cannot only create csi on tables")
        for c in self.index_cols:
            v = getattr(table.table.cols, c, None)
            if v is not None:
                if reindex or v.is_indexed and not v.index.is_csi:
                    v.remove_index()
                    v.create_csindex()
                elif not v.is_indexed:
                    v.create_csindex()

    def _split_dict(self, columns):
        """
        This function creates a dictionary describing a mapping from columns to tables. The keys correspond to tables
        and the values are lists of column names which will be saved into the corresponding table.

        :param columns:
        :return:
        """
        # TODO write issue to pandas hdfstore failes silently if appending to multiple and using non existent keys
        if isinstance(self.split, dict):
            return self.split
        keys = self._build_subtable_keys()
        if isinstance(self.split, int):
            column_splits = np.array_split(columns, self.split)
            return dict(zip(keys, column_splits))
        elif self.split == "auto":
            len(columns)
        else:
            return dict(zip(keys, self.split))

    def _check_key_integrity(self, keys, store):
        """
        Adds security to pandas append method which fails silently if appending to non existent keys

        :return:
        """
        for k in keys:
            if not self.fs.exists(k, store):
                raise KeyError("Could not find key: {} in store".format(k))

    def _build_subtable_keys(self):
        keys = [os.path.join(self.key, "subtable_{}".format(idx)) for idx in range(self.split)]
        keys[0] = os.path.join(self.key, self.selector)
        return keys


class AtomicHdf5Table(Hdf5Table):
    def __init__(self, *args, **kwargs):
        """
        Class to represent an atomic table that will not exist unless all operations on it are finished successfully

        :param args:
        :param overwrite: if true existing keys will be overwritten
        :param kwargs:
        """
        overwrite = kwargs.pop("overwrite", False)
        super(AtomicHdf5Table, self).__init__(*args, **kwargs)
        self.final_dest = self.key
        if not self.fs.exists(self.final_dest):
            self.key = tempfile.mkdtemp()

        mode = kwargs.pop("mode", "r")
        if self.fs.exists(self.final_dest) and overwrite is False and mode in "wa":
            raise ValueError(
                "Key {} is already in store {} write operation cannot ensure atomicity. Set overwrite to True"
                "to overwrite the table.".format(self.final_dest, self.fn))
        elif self.fs.exists(self.final_dest) and overwrite is True and mode in "wa":
            raise NotImplementedError("Overwriting tables in same store is not implemented yet")

    def _move(self, node, dest):
        try:
            self.fs.move(node, dest)
        except NoSuchNodeError:
            nodes_str = "\n".join(self.fs.listdir("/"))
            LOGGER.error("Error while moving node. Current Node structure{}".format(nodes_str))
            raise

    def _move_to_final_destination(self):
        if self.mode in "wa" and self.written:
            self._move(self.key, self.final_dest)

    def close(self, exc=False):
        super(AtomicHdf5Table, self).close(exc)
        if not exc:
            self._move_to_final_destination()


def sorted_table_iterator(res):
    it = iter(res)
    while 1:
        try:
            value = next(it).sort_index(axis=1)
        except StopIteration:
            return
        yield value
