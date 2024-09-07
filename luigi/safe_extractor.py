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
This module provides a class `SafeExtractor` that offers a secure way to extract tar files while
mitigating path traversal vulnerabilities, which can occur when files inside the archive are
crafted to escape the intended extraction directory.

The `SafeExtractor` ensures that the extracted file paths are validated before extraction to
prevent malicious archives from extracting files outside the intended directory.

Classes:
    SafeExtractor: A class to securely extract tar files with protection against path traversal attacks.

Usage Example:
    extractor = SafeExtractor("/desired/directory")
    extractor.safe_extract("archive.tar")
"""

import os
import tarfile


class SafeExtractor:
    """
    A class to safely extract tar files, ensuring that no path traversal
    vulnerabilities are exploited.

    Attributes:
        path (str): The directory to extract files into.

    Methods:
        _is_within_directory(directory, target):
            Checks if a target path is within a given directory.

        safe_extract(tar_path, members=None, \\*, numeric_owner=False):
            Safely extracts the contents of a tar file to the specified directory.
    """

    def __init__(self, path="."):
        """
        Initializes the SafeExtractor with the specified directory path.

        Args:
            path (str): The directory to extract files into. Defaults to the current directory.
        """
        self.path = path

    @staticmethod
    def _is_within_directory(directory, target):
        """
        Checks if a target path is within a given directory.

        Args:
            directory (str): The directory to check against.
            target (str): The target path to check.

        Returns:
            bool: True if the target path is within the directory, False otherwise.
        """
        abs_directory = os.path.abspath(directory)
        abs_target = os.path.abspath(target)
        prefix = os.path.commonprefix([abs_directory, abs_target])
        return prefix == abs_directory

    def safe_extract(self, tar_path, members=None, *, numeric_owner=False):
        """
        Safely extracts the contents of a tar file to the specified directory.

        Args:
            tar_path (str): The path to the tar file to extract.
            members (list, optional): A list of members to extract. Defaults to None.
            numeric_owner (bool, optional): If True, only the numeric owner will be used. Defaults to False.

        Raises:
            RuntimeError: If a path traversal attempt is detected.
        """
        with tarfile.open(tar_path, 'r') as tar:
            for member in tar.getmembers():
                member_path = os.path.join(self.path, member.name)
                if not self._is_within_directory(self.path, member_path):
                    raise RuntimeError("Attempted Path Traversal in Tar File")
            tar.extractall(self.path, members, numeric_owner=numeric_owner)
