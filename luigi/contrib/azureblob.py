# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 Microsoft Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import os
import tempfile
import logging
import datetime

from azure.storage.blob import blockblobservice

from luigi.format import get_default_format
from luigi.target import FileAlreadyExists, FileSystem, AtomicLocalFile, FileSystemTarget

logger = logging.getLogger('luigi-interface')


class AzureBlobClient(FileSystem):
    """
    Create an Azure Blob Storage client for authentication.
    Users can create multiple storage account, each of which acts like a silo. Under each storage account, we can
    create a container. Inside each container, the user can create multiple blobs.

    For each account, there should be an account key. This account key cannot be changed and one can access all the
    containers and blobs under this account using the account key.

    Usually using an account key might not always be the best idea as the key can be leaked and cannot be revoked. The
    solution to this issue is to create Shared `Access Signatures` aka `sas`. A SAS can be created for an entire
    container or just a single blob. SAS can be revoked.
    """
    def __init__(self, account_name=None, account_key=None, sas_token=None, **kwargs):
        """
        :param str account_name:
            The storage account name. This is used to authenticate requests signed with an account key\
            and to construct the storage endpoint. It is required unless a connection string is given,\
            or if a custom domain is used with anonymous authentication.
        :param str account_key:
            The storage account key. This is used for shared key authentication.
        :param str sas_token:
            A shared access signature token to use to authenticate requests instead of the account key.
        :param dict kwargs:
            A key-value pair to provide additional connection options.

            * `protocol` - The protocol to use for requests. Defaults to https.
            * `connection_string` - If specified, this will override all other parameters besides request session.\
                See http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/ for the connection string format
            * `endpoint_suffix` - The host base component of the url, minus the account name. Defaults to Azure\
                (core.windows.net). Override this to use the China cloud (core.chinacloudapi.cn).
            * `custom_domain` - The custom domain to use. This can be set in the Azure Portal. For example, ‘www.mydomain.com’.
            * `token_credential` - A token credential used to authenticate HTTPS requests. The token value should be updated before its expiration.
        """
        self.options = {"account_name": account_name, "account_key": account_key, "sas_token": sas_token}
        self.kwargs = kwargs

    @property
    def connection(self):
        return blockblobservice.BlockBlobService(account_name=self.options.get("account_name"),
                                                 account_key=self.options.get("account_key"),
                                                 sas_token=self.options.get("sas_token"),
                                                 protocol=self.kwargs.get("protocol"),
                                                 connection_string=self.kwargs.get("connection_string"),
                                                 endpoint_suffix=self.kwargs.get("endpoint_suffix"),
                                                 custom_domain=self.kwargs.get("custom_domain"),
                                                 is_emulated=self.kwargs.get("is_emulated") or False)

    def upload(self, tmp_path, container, blob, **kwargs):
        logging.debug("Uploading file '{tmp_path}' to container '{container}' and blob '{blob}'".format(
            tmp_path=tmp_path, container=container, blob=blob))
        self.create_container(container)
        lease_id = self.connection.acquire_blob_lease(container, blob)\
            if self.exists("{container}/{blob}".format(container=container, blob=blob)) else None
        try:
            self.connection.create_blob_from_path(container, blob, tmp_path, lease_id=lease_id, progress_callback=kwargs.get("progress_callback"))
        finally:
            if lease_id is not None:
                self.connection.release_blob_lease(container, blob, lease_id)

    def download_as_bytes(self, container, blob, bytes_to_read=None):
        start_range, end_range = (0, bytes_to_read-1) if bytes_to_read is not None else (None, None)
        logging.debug("Downloading from container '{container}' and blob '{blob}' as bytes".format(
            container=container, blob=blob))
        return self.connection.get_blob_to_bytes(container, blob, start_range=start_range, end_range=end_range).content

    def download_as_file(self, container, blob, location):
        logging.debug("Downloading from container '{container}' and blob '{blob}' to {location}".format(
            container=container, blob=blob, location=location))
        return self.connection.get_blob_to_path(container, blob, location)

    def create_container(self, container_name):
        return self.connection.create_container(container_name)

    def delete_container(self, container_name):
        lease_id = self.connection.acquire_container_lease(container_name)
        self.connection.delete_container(container_name, lease_id=lease_id)

    def exists(self, path):
        container, blob = self.splitfilepath(path)
        return self.connection.exists(container, blob)

    def remove(self, path, recursive=True, skip_trash=True):
        container, blob = self.splitfilepath(path)
        if not self.exists(path):
            return False
        lease_id = self.connection.acquire_blob_lease(container, blob)
        self.connection.delete_blob(container, blob, lease_id=lease_id)
        return True

    def mkdir(self, path, parents=True, raise_if_exists=False):
        container, blob = self.splitfilepath(path)
        if raise_if_exists and self.exists(path):
            raise FileAlreadyExists("The Azure blob path '{blob}' already exists under container '{container}'".format(
                blob=blob, container=container))

    def isdir(self, path):
        """
        Azure Blob Storage has no concept of directories. It always returns False
        :param str path: Path of the Azure blob storage
        :return: False
        """
        return False

    def move(self, path, dest):
        try:
            return self.copy(path, dest) and self.remove(path)
        except IOError:
            self.remove(dest)
            return False

    def copy(self, path, dest):
        source_container, source_blob = self.splitfilepath(path)
        dest_container, dest_blob = self.splitfilepath(dest)
        if source_container != dest_container:
            raise Exception(
                "Can't copy blob from '{source_container}' to '{dest_container}'. File can be moved within container".format(
                    source_container=source_container, dest_container=dest_container
                ))

        source_lease_id = self.connection.acquire_blob_lease(source_container, source_blob)
        destination_lease_id = self.connection.acquire_blob_lease(dest_container, dest_blob) if self.exists(dest) else None
        try:
            return self.connection.copy_blob(source_container, dest_blob, self.connection.make_blob_url(
                source_container, source_blob),
                destination_lease_id=destination_lease_id, source_lease_id=source_lease_id)
        finally:
            self.connection.release_blob_lease(source_container, source_blob, source_lease_id)
            if destination_lease_id is not None:
                self.connection.release_blob_lease(dest_container, dest_blob, destination_lease_id)

    def rename_dont_move(self, path, dest):
        self.move(path, dest)

    @staticmethod
    def splitfilepath(filepath):
        splitpath = filepath.split("/")
        container = splitpath[0]
        blobsplit = splitpath[1:]
        blob = None if not blobsplit else "/".join(blobsplit)
        return container, blob


class ReadableAzureBlobFile:
    def __init__(self, container, blob, client, download_when_reading, **kwargs):
        self.container = container
        self.blob = blob
        self.client = client
        self.closed = False
        self.download_when_reading = download_when_reading
        self.azure_blob_options = kwargs
        self.download_file_location = os.path.join(tempfile.mkdtemp(prefix=str(datetime.datetime.utcnow())), blob)
        self.fid = None

    def read(self, n=None):
        return self.client.download_as_bytes(self.container, self.blob, n)

    def __enter__(self):
        if self.download_when_reading:
            self.client.download_as_file(self.container, self.blob, self.download_file_location)
            self.fid = open(self.download_file_location)
            return self.fid
        else:
            return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __del__(self):
        self.close()
        if os._exists(self.download_file_location):
            os.remove(self.download_file_location)

    def close(self):
        if self.download_when_reading:
            if self.fid is not None and not self.fid.closed:
                self.fid.close()
                self.fid = None

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def seek(self, offset, whence=None):
        pass


class AtomicAzureBlobFile(AtomicLocalFile):
    def __init__(self, container, blob, client, **kwargs):
        super(AtomicAzureBlobFile, self).__init__(os.path.join(container, blob))
        self.container = container
        self.blob = blob
        self.client = client
        self.azure_blob_options = kwargs

    def move_to_final_destination(self):
        self.client.upload(self.tmp_path, self.container, self.blob, **self.azure_blob_options)


class AzureBlobTarget(FileSystemTarget):
    """
    Create an Azure Blob Target for storing data on Azure Blob Storage
    """
    def __init__(self, container, blob, client=None, format=None, download_when_reading=True, **kwargs):
        """
        :param str account_name:
            The storage account name. This is used to authenticate requests signed with an account key and to construct
            the storage endpoint. It is required unless a connection string is given, or if a custom domain is
            used with anonymous authentication.
        :param str container:
            The azure container in which the blob needs to be stored
        :param str blob:
            The name of the blob under container specified
        :param str client:
            An instance of :class:`.AzureBlobClient`. If none is specified, anonymous access would be used
        :param str format:
            An instance of :class:`luigi.format`.
        :param bool download_when_reading:
            Determines whether the file has to be downloaded to temporary location on disk. Defaults to `True`.

        Pass the argument **progress_callback** with signature *(func(current, total))* to get real time progress of upload
        """
        super(AzureBlobTarget, self).__init__(os.path.join(container, blob))
        if format is None:
            format = get_default_format()
        self.container = container
        self.blob = blob
        self.client = client or AzureBlobClient()
        self.format = format
        self.download_when_reading = download_when_reading
        self.azure_blob_options = kwargs

    @property
    def fs(self):
        """
        The :py:class:`FileSystem` associated with :class:`.AzureBlobTarget`
        """
        return self.client

    def open(self, mode):
        """
        Open the target for reading or writing

        :param char mode:
            'r' for reading and 'w' for writing.

            'b' is not supported and will be stripped if used. For binary mode, use `format`
        :return:
            * :class:`.ReadableAzureBlobFile` if 'r'
            * :class:`.AtomicAzureBlobFile` if 'w'
        """
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)
        if mode == 'r':
            return self.format.pipe_reader(ReadableAzureBlobFile(self.container, self.blob, self.client, self.download_when_reading, **self.azure_blob_options))
        else:
            return self.format.pipe_writer(AtomicAzureBlobFile(self.container, self.blob, self.client, **self.azure_blob_options))
