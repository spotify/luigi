import luigi


class UnimportedTask(luigi.Task):
    def complete(self):
        return False
