# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Implementation of Azure Data Lake Store support. 
ADLTarget subclasses the base Target class to support storing files on 
Microsoft Azure's distributed file store Azure Data Lake. 
The azure python sdk library is required to support this functionality.

Requires an Azure Service Principal for authentication.
See: https://docs.microsoft.com/en-us/cli/azure/create-an-azure-service-principal-azure-cli?view=azure-cli-latest

Provide the following in your luigi config file

[adl]
az_tenant_id={YOUR_TENANT_ID}
az_sp_client_id={YOUR_SERVICE_PRINCIPAL_ID}
az_sp_client_secret={YOUR_SERVICE_PRINCIPAL_SECRET}
adl_store_name={YOUR_AZURE_DATA_LAKE_STORE_NAME}
"""

import logging

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError

import luigi
from luigi import configuration, six

logger = logging.getLogger('luigi-interface')


class FileNotFoundException(luigi.target.FileSystemException):
    pass


class ADLClient(luigi.target.FileSystem):
    """
    Azure SDK powered Azure Data Lake store client.
    """

    _adl = None

    def __init__(self, 
                 az_tenant_id=None,
                 az_sp_client_id=None,
                 az_sp_client_secret=None,
                 adl_store_name=None,
                 **kwargs):
        """
        Initialize configuration for Azure Data Lake Client
        :param az_tenant_id: Active Directory TenantID
        :param az_sp_client_id: Azure Service Principal Id
        :param az_sp_client_secret: Azure Service Principal Secret Key
        :param kwargs: extra config options
        """
        options = self._get_adl_config()
        options.update(kwargs)
        if az_tenant_id:
            options['az_tenant_id'] = az_tenant_id
        if az_sp_client_id:
            options['az_sp_client_id'] = az_sp_client_id
        if az_sp_client_secret:
            options['az_sp_client_secret'] = az_sp_client_secret
        if adl_store_name:
            options['adl_store_name'] = adl_store_name

        self._options = options

    @property
    def adl(self):
        """
        Create the Azure Data Lake client singleton with the provided configuration
        """
        from azure.datalake.store import core, lib

        options = dict(self._options)

        if self._adl:
            return self._adl

        az_tenant_id = options.get('az_tenant_id')
        az_sp_client_id = options.get('az_sp_client_id')
        az_sp_client_secret = options.get('az_sp_client_secret')
        adl_store_name = options.get('adl_store_name')

        for key in ['az_tenant_id', 'az_sp_client_id', 'az_sp_client_secret', 'adl_store_name']:
            if key in options:
                options.pop(key)

        token = lib.auth(tenant_id=az_tenant_id,
                 client_id=az_sp_client_id,
                 client_secret=az_sp_client_secret,
                 **options)
        self._adl = core.AzureDLFileSystem(token, store_name=adl_store_name)
        return self._adl

    @adl.setter
    def adl(self, value):
        self._adl = value

    def exists(self, path):
        """
        Does the path exist
        :param path: path to file for luigi to check
        :return: boolean (file exists)
        """
        return self.adl.exists(path, invalidate_cache=True)

    def listdir(self, path):
        """
        List contents of directory provided by path
        :param path: directory to list
        :return: array of file/directory names at this path
        """
        return self.adl.ls(path, detail=True, invalidate_cache=True)

    def open(self, path, mode='rb', blocksize=2**25, delimiter=None):
        """
        Open a file for reading or writing in bytes mode
        :param path:
        :param mode:
        :param blocksize:
        :param delimiter:
        :return:
        """
        return self.adl.open(path, mode=mode, blocksize=blocksize, delimiter=delimiter)

    def put(self, source_path, destination_path, delimiter=None):
        """
        Upload a file from local path to Azure Data Lake store
        :param source_path: path to local file
        :param destination_path: path to destination file on ADL
        :param delimiter: file delimiter (e.g. ',' for csv)
        """
        self.adl.put(source_path, destination_path, delimiter=delimiter)

    def put_multipart(self, source_path, destination_path, thread_count,
                      overwrite=False, chunksize=2**28, buffersize=2**22,
                      blocksize=2**22, show_progress_bar=False):
        """
        Use multithread uploader with progress callback for very large files
        :param source_path: path to local file
        :param destination_path: path to destination file on ADL
        :param thread_count: threads to use
        :param overwrite: overwrite if file exists (defaults to False)
        :param chunksize: file chunksize
        :param buffersize:
        :param blocksize:
        :param show_progress_bar: Show a progress bar with Azure cli's controller
        """
        if show_progress_bar:
            try:
                import cli
            except:
                raise ImportError('Please install the azure cli pip package to show upload progress')

            from azure.cli.core.application import APPLICATION

            def _update_progress(current, total):
                hook = APPLICATION.get_progress_controller(det=True)
                hook.add(message='Alive', value=current, total_val=total)
                if total == current:
                    hook.end()
        else:
            def _update_progress(current, total):
                print('{}% complete'.format(round(current / total, 2)))

        from azure.datalake.store.multithread import ADLUploader

        ADLUploader(self.adl, destination_path, source_path, thread_count, overwrite=overwrite,
                    chunksize=chunksize,
                    buffersize = buffersize,
                    blocksize = blocksize,
                    progress_callback = _update_progress
        )

    def remove(self, path, recursive=True, skip_trash=True):
        """
        Removes the provided file/directory path from ADL store
        :param path: path to delte
        :param recursive whether to remove subdirectories/files
        :param skip_trash skips ADL trash and removes completely
        """   
        return self.adl.rm(path, recursive=recursive)

    def _get_adl_config(self, key=None):
        """
        Get configuration from luigi cfg file.        
        :param key if provided, get only this key from config
        """
        defaults = dict(configuration.get_config().defaults())
        try:
            config = dict(configuration.get_config().items('adl'))
        except NoSectionError:
            return {}
        # So what ports etc can be read without us having to specify all dtypes
        for k, v in six.iteritems(config):
            try:
                config[k] = int(v)
            except ValueError:
                pass
        if key:
            return config.get(key)
        section_only = {k: v for k, v in config.items() if k not in defaults or v != defaults[k]}

        return section_only


class AtomicADLFile(luigi.target.AtomicLocalFile):
    """
    Writes to tmp file and puts to ADL on close
    **kwargs are any arguments you want to pass through to:
    azure.datalake.store.multithread.ADLUploader
    """
    def __init__(self, path, client, **kwargs):
        self.client = client
        super(AtomicADLFile, self).__init__(path)
        self.adl_options = kwargs

    def move_to_final_destination(self):
        self.client.put_multipart(self.tmp_path, self.path, **self.adl_options)


class ADLTarget(luigi.target.FileSystemTarget):
    """
    Target Azure Data Lake file object
    """
    fs = None

    def __init__(self, path, format=None, client=None, **kwargs):
        super(ADLTarget, self).__init__(path)
        if format is None:
            format = luigi.format.get_default_format()

        self.path = path
        self.format = format
        self.fs = client or ADLClient()
        self.adl_options = kwargs

    def open(self, mode='r'):
        """
        This will use the Azure Data Lake store implementation
        for opening/reading files which only return byte streams - rb|wb -
        not actual files but for consistency with luigi API, the mode
        is denoted as r|w
        """
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            if not self.fs.exists(self.path):
                raise FileNotFoundException("Could not find file at %s" % self.path)

            return self.fs.open(self.path)
        else:
            return self.format.pipe_writer(AtomicADLFile(self.path, self.fs, **self.adl_options))


class ADLPathTask(luigi.ExternalTask):
    """
    An external task that requires existence of a path in Azure Data Lake store.
    """
    path = luigi.Parameter()

    def output(self):
        return ADLTarget(self.path)
