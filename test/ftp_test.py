import ntpath
from types import SimpleNamespace
from unittest.mock import Mock, call, mock_open, patch

import pytest

from luigi.contrib import ftp


@pytest.mark.parametrize("atomic", [False, True])
@pytest.mark.parametrize("sftp", [False, True])
def test_upload_keeps_remote_paths_posix_on_windows(atomic, sftp):
    fs = ftp.RemoteFileSystem("example.invalid", sftp=sftp)
    fs.conn = Mock()
    fs.conn.nlst.return_value = []
    local_path = r"C:\data\report.txt"
    destination = "/reports/daily/report.txt"
    upload_path = "/reports/daily/luigi-tmp-000000042" if atomic else destination
    file_open = mock_open(read_data=b"report")

    with (
        patch.object(ftp, "os", SimpleNamespace(path=ntpath, sep="\\")),
        patch.object(ftp.random, "randrange", return_value=42),
        patch("builtins.open", file_open),
    ):
        if sftp:
            fs._sftp_put(local_path, "/reports/./daily/report.txt", atomic)
        else:
            fs._ftp_put(local_path, "/reports/./daily/report.txt", atomic)

    if sftp:
        fs.conn.makedirs.assert_called_once_with("/reports/daily")
        fs.conn.put.assert_called_once_with(local_path, upload_path)
    else:
        file_open.assert_called_once_with(local_path, "rb")
        fs.conn.mkd.assert_has_calls([call("reports"), call("daily")])
        fs.conn.storbinary.assert_called_once_with("STOR " + upload_path, file_open())

    if atomic:
        fs.conn.rename.assert_called_once_with(upload_path, destination)
    else:
        fs.conn.rename.assert_not_called()
