"""
Presents the ability to interact with HDFS over WebHDFS (or HttpFS) using
the whoops library. Provides compatiblity with similar functionality in
luigi.hdfs whenever possible.
"""
import datetime
import configuration
import os
import posixpath
import urlparse
import whoops


def get_whoops_defaults(config=None):
    """Reads defaults from a client configuration file and fails if not."""
    config = config or configuration.get_config()
    try:
        return {
            "host": config.get("hdfs", "namenode_host"),
            "port": config.get("hdfs", "namenode_port")
        }
    except:
        raise RuntimeError("You must specify namenode_host and namenode_port "
                           "in the [hdfs] section of your luigi config in "
                           "order to use luigi's whoops support without a "
                           "fully-qualified url")


def get_whoops(path, config=None):
    """gets an instance of whoops.WebHDFS for the given path. If path is not an absolute URI,
       then it uses the host and port from the configuration."""
    (scheme, netloc, path, query, fragment) = urlparse.urlsplit(path)

    if scheme and scheme != "hdfs" and scheme != "webhdfs":
        raise RuntimeError("only hdfs and webhdfs supported!")
    host = None
    port = None

    def parse_netloc(netloc):
        if ':' in netloc:
            return netloc.split(":")
        return (host, None)

    # If the path specifies a netloc (i.e. a host:port) then use it. Else, try
    # to use the defaults
    if netloc:
        if not ':' in netloc:
            raise RuntimeError("Malformed url. Must have a host:port netloc")
        (host, port) = netloc.split(':')
    else:
        defaults = get_whoops_defaults(config)
        host = defaults['host']
        port = defaults['port']

    return whoops.WebHDFS(host, port, user=os.environ['USER'] if 'USER' in os.environ else None)


class WebHdfsClient(object):
    """Hdfs Client that uses the `whoops` python library to communicate with webhdfs. In order to
    use the client, you must specify `namenode_host` and `namenode_port` in the `hdfs` section of
    your luigi configuration."""

    def __init__(self):
        "Configures _homedir so that we can handle relative paths"
        self._homedir = get_whoops("/").home()

    def _make_absolute(self, inpath):
        """Makes the given path absolute if it's not already. It is assumed that the path is
        relative to self._homedir"""
        (scheme, netloc, path, query, fragment) = urlparse.urlsplit(inpath)

        if scheme or posixpath.isabs(path):
            # if a scheme is specified, assume it's absolute.
            return path
        return posixpath.join(self._homedir, path)

    def exists(self, path):
        """Returns true if the path exists and false otherwise"""
        whdfs = get_whoops(path)
        try:
            whdfs.stat(self._make_absolute(path))
            return True
        except whoops.WebHDFSError, e:
            if e.args[0] == "Not Found":
                return False
            raise e

    def rename(self, path, dest):
        (scheme, netloc, _, _, _) = urlparse.urlsplit(path)
        (dest_scheme, dest_netloc, _, _, _) = urlparse.urlsplit(dest)
        if scheme != dest_scheme or netloc != dest_netloc:
            raise RuntimeError("Filesystems don't match. source: %s dest: %s".format(path, dest))

        return get_whoops(path).rename(self._make_absolute(path), self._make_absolute(dest))

    def remove(self, path, recursive=True):
        """Note that skip trash option doesn't exist -- trash is always skipped"""
        return get_whoops(path).delete(self._make_absolute(path), recursive)

    def mkdir(self, path):
        return get_whoops(path).mkdir(self._make_absolute(path))

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False):
        if not path:
            path = "."  # default to current/home catalog

        filestatuses = get_whoops(path).listdir(path)
        for fs in filestatuses:
            if ignore_directories and fs['type'] == 'DIRECTORY':
                continue
            if ignore_files and fs['type'] == 'FILE':
                continue
            file = posixpath.join(path, fs['pathSuffix'])
            extra_data = ()

            if include_size:
                extra_data += (fs['length'],)
            if include_type:
                # this is ugly but necessary to be compatible with hdfs.py
                extra_data += ('d' if fs['type'] == 'DIRECTORY' else '-',)
            if include_time:
                modification_time = datetime.datetime.fromtimestamp(fs[u'modificationTime']/1000)
                extra_data += (modification_time,)

            if len(extra_data) > 0:
                yield (file,) + extra_data
            else:
                yield file

client = WebHdfsClient()

exists = client.exists
rename = client.rename
remove = client.remove
mkdir = client.mkdir
listdir = client.listdir
