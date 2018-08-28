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
import logging
import os
import warnings

from .cfg_parser import LuigiConfigParser
from .toml_parser import LuigiTomlParser


logger = logging.getLogger('luigi-interface')


PARSERS = {
    'cfg': LuigiConfigParser,
    'conf': LuigiConfigParser,
    'ini': LuigiConfigParser,
    'toml': LuigiTomlParser,
}

# select parser via env var
DEFAULT_PARSER = 'cfg'
PARSER = os.environ.get('LUIGI_CONFIG_PARSER', DEFAULT_PARSER)
if PARSER not in PARSERS:
    warnings.warn("Invalid parser: {parser}".format(parser=PARSER))
    PARSER = DEFAULT_PARSER


def _check_parser(parser_class, parser):
    if not parser_class.enabled:
        msg = (
            "Parser not installed yet. "
            "Please, install luigi with required parser:\n"
            "pip install luigi[{parser}]"
        )
        raise ImportError(msg.format(parser=parser))


def get_config(parser=PARSER):
    """Get configs singleton for parser
    """
    parser_class = PARSERS[parser]
    _check_parser(parser_class, parser)
    return parser_class.instance()


def add_config_path(path):
    """Select config parser by file extension and add path into parser.
    """
    if not os.path.isfile(path):
        warnings.warn("Config file does not exist: {path}".format(path=path))
        return False

    # select parser by file extension
    _base, ext = os.path.splitext(path)
    if ext and ext[1:] in PARSERS:
        parser = ext[1:]
    else:
        parser = PARSER
    parser_class = PARSERS[parser]

    _check_parser(parser_class, parser)
    if parser != PARSER:
        msg = (
            "Config for {added} parser added, but used {used} parser. "
            "Set up right parser via env var: "
            "export LUIGI_CONFIG_PARSER={added}"
        )
        warnings.warn(msg.format(added=parser, used=PARSER))

    # add config path to parser
    parser_class.add_config_path(path)
    return True


if 'LUIGI_CONFIG_PATH' in os.environ:
    add_config_path(os.environ['LUIGI_CONFIG_PATH'])
