
import os
import logging
from ConfigParser import ConfigParser, NoOptionError, NoSectionError


class LuigiConfigParser(ConfigParser):
    NO_DEFAULT = object()
    _instance = None
    _config_paths = ['/etc/luigi/client.cfg', 'client.cfg']
    if 'LUIGI_CONFIG_PATH' in os.environ:
        _config_paths.append(os.environ['LUIGI_CONFIG_PATH'])

    @classmethod
    def add_config_path(cls, path):
        cls._config_paths.append(path)
        cls.reload()

    @classmethod
    def instance(cls, *args, **kwargs):
        """ Singleton getter """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            loaded = cls._instance.reload()
            logging.getLogger('luigi-interface').info('Loaded %r', loaded)

        return cls._instance

    @classmethod
    def reload(cls):
        return cls.instance().read(cls._config_paths)

    def _get_with_default(self, method, section, option, default, expected_type=None):
        """ Gets the value of the section/option using method. Returns default if value
        is not found. Raises an exception if the default value is not None and doesn't match
        the expected_type.
        """
        try:
            return method(self, section, option)
        except (NoOptionError, NoSectionError):
            if default is LuigiConfigParser.NO_DEFAULT:
                raise
            if expected_type is not None and default is not None and \
               not isinstance(default, expected_type):
                raise
            return default

    def get(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.get, section, option, default)

    def getboolean(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getboolean, section, option, default, bool)

    def getint(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getint, section, option, default, int)

    def getfloat(self, section, option, default=NO_DEFAULT):
        return self._get_with_default(ConfigParser.getfloat, section, option, default, float)

    def getintdict(self, section):
        try:
            return dict((key, int(value)) for key, value in self.items(section))
        except NoSectionError:
            return {}

    def set(self, section, option, value):
        if not ConfigParser.has_section(self, section):
            ConfigParser.add_section(self, section)

        return ConfigParser.set(self, section, option, value)

def get_config():
    """ Convenience method (for backwards compatibility) for accessing config singleton """
    return LuigiConfigParser.instance()
