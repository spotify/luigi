import json
import luigi.interface
import os


class QueueWorker(luigi.WrapperTask):
    ''' Parses args to create a luigi Task, and returns it as requires. Calls complete_callback
        when it finishes.
    '''
    args = luigi.Parameter(description="Task name and command line args for the task to run")
    complete_callback = luigi.Parameter(default=None, description="Callback for when task completes")
    metadata = luigi.Parameter(default=None, description="Allows QueueSchedulers to attach data to a worker")

    @property
    def task(self):
        try:
            return luigi.interface.ArgParseInterface().parse(cmdline_args=self.args)[0]
        except SystemExit as exc:
            # if parsing the task fails, remove from queue
            self.on_failure(exc)
            raise

    def complete(self):
        is_complete = self.task.complete()
        if is_complete:
            self.complete_callback(worker=self, success=True)
        return is_complete

    def run(self):
        luigi.interface.ArgParseInterface().run([self.task], {"workers": 1})

    def on_success(self):
        if self.complete_callback:
            self.complete_callback(worker=self, success=True)
        return super(QueueWorker, self).on_success()

    def on_failure(self, exception):
        if self.complete_callback:
            self.complete_callback(worker=self, success=False)
        return super(QueueWorker, self).on_failure(exception)


class QueueScheduler(luigi.WrapperTask):
    ''' Luigi 'daemon' for scheduling Tasks. Should return the same tasks on repeated invocation
        of fetch_queue_tasks, as long as they haven't been completed.
    '''
    def fetch_queue_tasks(self):
        ''' Return a list/generator of [string], or ([string], metadata):
                [string] - commandline arguments to invoke a luigi Task
                metadata - will be available on worker.metadata passed to worker_complete
        '''
        raise NotImplementedError

    def worker_complete(self, worker, success):
        ''' Will be called when a worker completes. Used to remove a task from the queue
        '''
        pass

    def complete(self):
        ''' Always run '''
        return False

    def requires(self):
        tasks = list(self.fetch_queue_tasks())

        if not tasks:
            return None

        if not isinstance(tasks[0], tuple):
            tasks = [(task, None) for task in tasks]

        return [QueueWorker(args=task_args, complete_callback=self.worker_complete, metadata=metadata)
                for (task_args, metadata) in tasks]


class HdfsFileQueueScheduler(QueueScheduler):
    ''' Queue scheduler that reads files starting with 'luigi-work'. Work is done in
        sorted order of the file names.
    '''
    queue_workers = luigi.IntParameter(default=1)
    work_directory = luigi.Parameter()
    done_directory = luigi.Parameter(default=None, description="Defaults to work_directory/done")
    failed_directory = luigi.Parameter(default=None, description="Defaults to work_directory/failed")

    def fetch_queue_tasks(self):
        files = luigi.hdfs.listdir(self.work_directory)
        work_files = [f for f in files if os.path.basename(f).startswith('luigi-work')]
        work_files.sort()
        for filename in work_files[0:self.queue_workers]:
            with luigi.hdfs.HdfsTarget(filename).open('r') as work_file:
                work_json = json.loads(work_file.read().strip())
                args = [work_json['task_name']] + work_json['task_args']
                yield (args, filename)

    def worker_complete(self, worker, success):
        if not self.done_directory:
            self.done_directory = os.path.join(self.work_directory, "done")
        if not self.failed_directory:
            self.failed_directory = os.path.join(self.work_directory, "failed")

        if success:
            luigi.hdfs.rename(worker.metadata, self.done_directory)
        else:
            luigi.hdfs.rename(worker.metadata, self.failed_directory)
