class Scheduler(object):
    def __init__(self):
        self.__scheduled = set()
        self.__schedule = []

    def add_target(self, target):
        if target.exists(): return
        if target in self.__scheduled: return

        task = target.get_task()

        self.__scheduled.add(target)

        self.add_task(task)

    def add_task(self, task):
        if task in self.__scheduled: return

        self.__scheduled.add(task)

        for target in task.get_input():
            self.add_target(target)

        self.__schedule.append(task)

    def run(self):
        print 'will run', self.__schedule
        for task in self.__schedule:
            task.run()
