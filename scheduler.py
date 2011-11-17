from rule import flatten

class Scheduler(object):
    def __init__(self):
        self.__scheduled = set()
        self.__schedule = []

    def add(self, rule):
        if rule.exists(): return
        if rule in self.__scheduled: return

        for rule_2 in flatten(rule.requires()):
            print 'rule_2:', rule_2
            self.add(rule_2)

        self.__schedule.append(rule)

    def run(self):
        print 'will run', self.__schedule
        for rule in self.__schedule:
            # check inputs again
            for rule_2 in flatten(rule.requires()):
                if not rule_2.exists():
                    print 'dependency', rule_2, 'does not exist for', rule
                    break
            else:
                rule.run()
