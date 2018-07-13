import os

from .cfg_parser import LuigiConfigParser
from .toml_parser import LuigiTomlParser


PARSERS = {
    'cfg': LuigiConfigParser,
    'ini': LuigiConfigParser,
    'toml': LuigiTomlParser,
}


def get_config(parser=os.environ.get('LUIGI_CONFIG_PARSER', 'cfg')):
    """
    Convenience method for accessing config singleton.
    """
    return PARSERS[parser].instance()
