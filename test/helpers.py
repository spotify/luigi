import functools


class with_config(object):
  """Decorator to override config settings for the length of a function. Example:

    >>> @with_config({'foo': {'bar': 'baz'}})
    >>> def test():
    >>>  print luigi.configuration.get_config.get("foo", "bar")
    >>> test()
    baz
  """

  def __init__(self, config):
    self.config = config

  def __call__(self, fun):
    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
      import luigi.configuration
      orig_conf = luigi.configuration.get_config()
      luigi.configuration.LuigiConfigParser._instance = None
      conf = luigi.configuration.get_config()
      for (section, settings) in self.config.iteritems():
        if not conf.has_section(section):
          conf.add_section(section)
        for (name, value) in settings.iteritems():
          conf.set(section, name, value)
      try:
        return fun(*args, **kwargs)
      finally:
        luigi.configuration.LuigiConfigParser._instance = orig_conf
    return wrapper
