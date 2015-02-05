import logging
import types

import luigi.target

logger = logging.getLogger('luigi-interface')


class CascadingClient(object):
    """
    A FilesystemClient that will cascade failing function calls through a list of clients.

    Which clients are used are specified at time of construction.
    """

    # This constant member is supposed to include all methods, feel free to add
    # methods here. If you want full control of which methods that should be
    # created, pass the kwarg to the constructor.
    ALL_METHOD_NAMES = ['exists', 'rename', 'remove', 'chmod', 'chown',
                        'count', 'copy', 'get', 'put', 'mkdir', 'listdir',
                        'isdir',
                        'rename_dont_move',
                        ]

    def __init__(self, clients, method_names=None):
        self.clients = clients
        if method_names is None:
            method_names = self.ALL_METHOD_NAMES

        for method_name in method_names:
            new_method = self._make_method(method_name)
            real_method = types.MethodType(new_method, self, type(self))
            setattr(self, method_name, real_method)

    @classmethod
    def _make_method(cls, method_name):
        def new_method(self, *args, **kwargs):
            return self._chained_call(method_name, *args, **kwargs)
        return new_method

    def _chained_call(self, method_name, *args, **kwargs):
        for i in range(len(self.clients)):
            client = self.clients[i]
            try:
                result = getattr(client, method_name)(*args, **kwargs)
                return result
            except luigi.target.FileSystemException:
                # For exceptions that are semantical, we must throw along
                raise
            except:
                is_last_iteration = (i + 1) >= len(self.clients)
                if is_last_iteration:
                    raise
                else:
                    logger.exception(
                        'The {0} failed to {1}, using fallback class {2}'
                        .format(client.__class__.__name__, method_name,
                                self.clients[i + 1].__class__.__name__))
