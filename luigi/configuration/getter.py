from .cfg_parser import LuigiConfigParser


def get_config():
    """
    Convenience method (for backwards compatibility) for accessing config singleton.
    """
    return LuigiConfigParser.instance()
