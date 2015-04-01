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

try:
    from setuptools import setup
except:
    from distutils.core import setup


def get_static_files(path):
    return [os.path.join(dirpath.replace("luigi/", ""), ext) 
            for (dirpath, dirnames, filenames) in os.walk(path)
            for ext in ["*.html", "*.js", "*.css", "*.png"]]


luigi_package_data = sum(map(get_static_files, ["luigi/static", "luigi/templates"]), [])

readme_note = """\
.. note::

   For the latest source, discussion, etc, please visit the
   `GitHub repository <https://github.com/spotify/luigi>`_\n\n
"""

with open('README.rst') as fobj:
    long_description = readme_note + fobj.read()

install_requires = [
    'pyparsing',
    'tornado',
    'python-daemon',
]

if os.environ.get('READTHEDOCS', None) == 'True':
    install_requires.append('sqlalchemy')
    # So that we can build documentation for luigi.db_task_history and luigi.contrib.sqla

if sys.version_info[:2] < (2, 7):
    install_requires.extend(['argparse', 'ordereddict', 'importlib'])

setup(
    name='luigi',
    version='1.1.3',
    description='Workflow mgmgt + task scheduling + dependency resolution',
    long_description=long_description,
    author='Erik Bernhardsson',
    author_email='erikbern@spotify.com',
    url='https://github.com/spotify/luigi',
    license='Apache License 2.0',
    packages=[
        'luigi',
        'luigi.contrib',
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
        ]
    },
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: System :: Monitoring',
    ],
)
