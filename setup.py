# Copyright (c) 2012 Spotify AB
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

import os

try:
    from setuptools import setup
except:
    from distutils.core import setup

try:
    long_description = []
    for line in open('README.md'):
        # Strip all images from the pypi description
        if not line.startswith('!') and not line.startswith('```'):
            long_description.append(line)
except IOError:
    # Temporary workaround for pip install
    # See https:/x/github.com/spotify/luigi/issues/46
    long_description = ''

luigi_package_data = [os.path.join(dirpath.replace("luigi/", ""), ext)
                      for (dirpath, dirnames, filenames) in os.walk("luigi/static")
                      for ext in ["*.html", "*.js", "*.css", "*.png"]]

setup(name='luigi',
      version='1.0.4',
      description='Workflow mgmgt + task scheduling + dependency resolution',
      long_description=''.join(long_description),
      author='Erik Bernhardsson',
      author_email='erikbern@spotify.com',
      url='https://github.com/spotify/luigi',
      packages=[
        'luigi'
        ],
      package_data={
        'luigi': luigi_package_data
        },
      scripts=[
        'bin/luigid'
        ]
      )
