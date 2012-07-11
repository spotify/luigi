import StringIO
import target


class MockFile(target.Target):
    _file_contents = {}

    def __init__(self, fn):
        self.__fn = fn

    def exists(self,):
        return self.__fn in MockFile._file_contents

    def open(self, mode):
        fn = self.__fn

        class StringBuffer(StringIO.StringIO):
            # Just to be able to do writing + reading from the same buffer
            def close(self):
                if mode == 'w':
                    MockFile._file_contents[fn] = self.getvalue()
                StringIO.StringIO.close(self)

        if mode == 'w':
            return StringBuffer()
        else:
            return StringBuffer(MockFile._file_contents[fn])
