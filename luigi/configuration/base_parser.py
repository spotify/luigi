import logging


class BaseParser(object):
    _instance = None
    _config_paths = ()

    @classmethod
    def instance(cls, *args, **kwargs):
        """ Singleton getter """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            loaded = cls._instance.reload()
            logging.getLogger('luigi-interface').info('Loaded %r', loaded)

        return cls._instance

    @classmethod
    def add_config_path(cls, path):
        cls._config_paths.append(path)
        cls.reload()

    @classmethod
    def reload(cls):
        return cls.instance().read(cls._config_paths)
