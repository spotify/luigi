class Target(object):  # interface
    def exists(self):
        raise NotImplementedError

    def open(self, mode):
        raise NotImplementedError
