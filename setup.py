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
    import textwrap

    long_description = ['NOTE: For the latest code and documentation, please go to https://github.com/spotify/luigi', '']

    for line in open('README.md'):
        # Strip all images from the pypi description
        if not line.startswith('!') and not line.startswith('```'):
            for line in textwrap.wrap(line, 120, drop_whitespace=False):
                long_description.append(line)

    long_description = '\n'.join(long_description)

except Exception, e:
    import traceback
    traceback.print_exc()
    # Temporary workaround for pip install
    # See https:/x/github.com/spotify/luigi/issues/46
    long_description = ''

luigi_package_data = [os.path.join(dirpath.replace("luigi/", ""), ext)
                      for (dirpath, dirnames, filenames) in os.walk("luigi/static")
                      for ext in ["*.html", "*.js", "*.css", "*.png"]]

setup(name='luigi',
      version='1.0.6',
      description='Workflow mgmgt + task scheduling + dependency resolution',
      long_description=long_description,
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
