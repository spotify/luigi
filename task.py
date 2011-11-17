import target

class Task(object):
    # Wrapper class around the Rule class to make that one less cluttered
    # And to make Rule a proper interface class
    def __init__(self, rule_inst, args, kwargs):
        self.__rule_inst = rule_inst
        self.__args = args
        self.__kwargs = kwargs
        self.__inputs = []
        self.__outputs = []

    def add_input(self, input):
        self.__inputs.append(input)
        return input

    def add_output(self, output):
        output = target.Target(output, self) # adds a dependency back to this object
        self.__outputs.append(output)
        return output

    def get_input(self):
        self.__rule_inst.get_input(*self.__args, **self.__kwargs)
        return self.__inputs

    def get_output(self):
        self.__rule_inst.get_output(*self.__args, **self.__kwargs)
        return self.__outputs

    def run(self):
        return self.__rule_inst.run(*self.__args, **self.__kwargs)
