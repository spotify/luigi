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

from azure.storage.blob import blockblobservice

from luigi.format import get_default_format
from luigi.target import FileAlreadyExists, FileSystem, AtomicLocalFile, FileSystemTarget


class AzureBlobClient(FileSystem):
    def __init__(self,  **kwargs):
        """
        Create an Azure Blob Storage client using anonymous authentication
        """
        self.kwargs = kwargs

    def __init__(self, account_name, account_key, **kwargs):
        """
        Create an Azure Blob Storage client using account_name and account_key for authentication
        :param account_name: The storage account name. This is used to authenticate requests signed with an account key
            and to construct the storage endpoint. It is required unless a connection string is given, or if a custom
            domain is used with anonymous authentication.
        :param account_key: The storage account key. This is used for shared key authentication.
        """
        self.options = {"account_name": account_name, "account_key": account_key}
        self.kwargs = kwargs

    def __init__(self, sas_token, **kwargs):
        """
        Create an Azure Blob Storage client using sas_token for authentication
        :param sas_token: A shared access signature token to use to authenticate requests instead of the account key.
        """
        self.options = {"sas_token": sas_token}
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
                                                 token_credential=self.kwargs.get("token_credential"))

    def upload(self, tmp_path, container, blob, **kwargs):
        try:
            lease_id = self.connection.acquire_blob_lease(container, blob)
            self.connection.create_blob_from_path(container, blob, tmp_path, lease_id=lease_id, progress_callback=kwargs.get("progress_callback"))
        finally:
            self.connection.release_blob_lease(container, blob, lease_id)

    def download_as_bytes(self, container, blob):
        return self.connection.get_blob_to_bytes(container, blob).content

    def exists(self, path):
        try:
            container, blob = self.splitpath(path)
            exists_answer = self.connection.exists(container, blob)
            return exists_answer
        except Exception:
            return False

    def remove(self, path, recursive=True, skip_trash=True):
        container, blob = self.splitpath(path)
        if not self.exists(path):
            return False
        self.connection.delete_blob(container, blob)
        return True

    def mkdir(self, path, parents=True, raise_if_exists=False):
        container, blob = self.splitfilepath(path)
        if raise_if_exists and self.exists(path):
            raise FileAlreadyExists("The Azure blob path '{blob}' already exists under container '{container}'".format(
                blob=blob, contaniner=container))

    def isdir(self, path):
        """
        Azure Blob Storage has no concept of directories. It always returns False
        :param path: Path of the Azure blob storage
        :return: False
        """
        return False

    def move(self, path, dest):
        self.copy(path, dest)
        self.remove(self, path)

    def copy(self, path, dest):
        source_container, source_blob = self.splitfilepath(path)
        dest_container, dest_blob = self.splitfilepath(dest)
        if source_container is not dest_container:
            raise Exception(
                "Can't copy blob from '{source_container}' to '{dest_container}'. File can be moved within container".format(
                    source_container=source_container, dest_container=dest_container
                ))
        try:
            source_lease_id = self.connection.acquire_blob_lease(source_container, source_blob)
            destination_lease_id = self.connection.acquire_blob_lease(dest_container, dest_blob)
            self.connection.copy_blob(source_container, source_blob, dest_blob,
                                      destination_lease_id=destination_lease_id, source_lease_id=source_lease_id)
        finally:
            self.connection.release_blob_lease(source_container, source_blob, source_lease_id)
            self.connection.release_blob_lease(dest_container, dest_blob, destination_lease_id)

    def rename_dont_move(self, path, dest):
        self.move(path, dest)

    def splitfilepath(self, filepath):
        splitpath = filepath.split("/")
        container = splitpath[0]
        blob = "/".join(splitpath[1:])
        return container, blob


class ReadableAzureBlobFile(object):
    def __init__(self, container, blob, client, **kwargs):
        self.container = container
        self.blob = blob
        self.client = client
        self.closed = False
        self.azure_blob_options = kwargs

    def read(self):
        return self.client.download_as_bytes(self.container, self.blob)

    def close(self):
        self.closed = True

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __enter__(self):
        return self

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False


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
    fs = None

    def __init__(self, container, blob, client=None, format=None, **kwargs):
        """
        Create an Azure Blob Storage client using account_name and account_key for authentication
        :param account_name: The storage account name. This is used to authenticate requests signed with an account key
            and to construct the storage endpoint. It is required unless a connection string is given, or if a custom
            domain is used with anonymous authentication.
        :param container: The azure container in which the blob needs to be stored
        :param blob: The name of the blob under container specified
        :param client: An instance of py:class:`AzureBlobClient`. If none is specified, anonymous access would be used
        :param format: An instance of py:class:`Format`.
        """
        super(AzureBlobTarget, self).__init__(os.path.join(container, blob))
        if format is None:
            format = get_default_format()

        self.container = container
        self.blob = blob
        self.fs = client or AzureBlobClient()
        self.format = format
        self.azure_blob_options = kwargs

    def open(self, mode):
        """
        Open the target for reading or writing
        :param mode: 'r' for reading and 'w' for writing. 'b' is not supported. Use format in constructor
        :return: py:class`ReadableAzureBlobFile` if 'r' else py:class`AtomicAzureBlobFile`
        """
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)
        if mode == 'r':
            return self.format.pipe_reader(ReadableAzureBlobFile(self.container, self.blob, self.fs, **self.azure_blob_options))
        else:
            return self.format.pipe_writer(AtomicAzureBlobFile(self.container, self.blob, self.fs, **self.azure_blob_options))
