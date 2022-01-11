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
import sys

from setuptools import setup


def get_static_files(path):
    return [os.path.join(dirpath.replace("luigi/", ""), ext)
            for (dirpath, dirnames, filenames) in os.walk(path)
            for ext in ["*.html", "*.js", "*.css", "*.png",
                        "*.eot", "*.svg", "*.ttf", "*.woff", "*.woff2"]]


luigi_package_data = sum(map(get_static_files, ["luigi/static", "luigi/templates"]), [])

readme_note = """
.. note::

   For the latest source, discussion, etc, please visit the
   `GitHub repository <https://github.com/spotify/luigi>`_
"""

with open('README.rst') as fobj:
    long_description = "\n\n" + readme_note + "\n\n" + fobj.read()

install_requires = ['python-dateutil>=2.7.5,<3', 'tenacity>=6.3.0,<7']

# Can't use python-daemon>=2.2.0 if on windows
#     See https://pagure.io/python-daemon/issue/18
if sys.platform == 'nt':
    install_requires.append('python-daemon<2.2.0')
else:
    install_requires.append('python-daemon')

# Start from tornado 6, the minimum supported Python version is 3.5.2.
if sys.version_info[:3] >= (3, 5, 2):
    install_requires.append('tornado>=5.0,<7')
else:
    install_requires.append('tornado>=5.0,<6')

# Note: To support older versions of setuptools, we're explicitly not
#   using conditional syntax (i.e. 'enum34>1.1.0;python_version<"3.4"').
#   This syntax is a problem for setuptools as recent as `20.1.1`,
#   published Feb 16, 2016.
if sys.version_info[:2] < (3, 4):
    install_requires.append('enum34>1.1.0')

if os.environ.get('READTHEDOCS', None) == 'True':
    # So that we can build documentation for luigi.db_task_history and luigi.contrib.sqla
    install_requires.append('sqlalchemy')
    # readthedocs don't like python-daemon, see #1342
    install_requires = [x for x in install_requires if not x.startswith('python-daemon')]
    install_requires.append('sphinx>=1.4.4')  # Value mirrored in doc/conf.py

# load meta package infos
meta = {}
with open("luigi/__meta__.py", "r") as f:
    exec(f.read(), meta)

setup(
    name='luigi',
    version=meta['__version__'],
    description=meta['__doc__'].strip(),
    long_description=long_description,
    author=meta['__author__'],
    url=meta['__contact__'],
    license=meta['__license__'],
    packages=[
        'luigi',
        'luigi.configuration',
        'luigi.contrib',
        'luigi.contrib.hdfs',
        'luigi.tools'
    ],
    package_data={
        'luigi': luigi_package_data
    },
    entry_points={
        'console_scripts': [
            'luigi = luigi.cmdline:luigi_run',
            'luigid = luigi.cmdline:luigid',
            'luigi-grep = luigi.tools.luigi_grep:main',
            'luigi-deps = luigi.tools.deps:main',
            'luigi-deps-tree = luigi.tools.deps_tree:main'
        ]
    },
    install_requires=install_requires,
    extras_require={
        'prometheus': ['prometheus-client==0.5.0'],
        'toml': ['toml<2.0.0'],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: System :: Monitoring',
    ],
)
