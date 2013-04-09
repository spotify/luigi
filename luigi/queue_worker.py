import json
import luigi.interface
import os


class TaskDeserializer(object):
    ''' Return a luigi.Task from a name, arguments, and module. Raise a DeserializerException on failure
    '''
    def deserialize(self, task_name, task_args, module_name=None):
        pass


class DeserializerException(Exception):
    pass


class ArgParseTaskDeserializer(TaskDeserializer):
    def deserialize(self, task_name, task_args, module_name=None):
        if module_name:
            __import__(module_name)
        try:
            return luigi.interface.ArgParseInterface().parse([task_name] + task_args)[0]
        except (SystemExit, Exception):  # convert SystemExit to regular Exception
            raise DeserializerException("Failed to deserialize (task_name=%s, task_args=%s, module_name=%s"
                            % (task_name, task_args, module_name))


class QueueWorker(luigi.WrapperTask):
    ''' Parses args to create a luigi Task, and returns it as requires. Calls complete_callback
        when it finishes.
    '''
    task = luigi.Parameter(description="Task instance for the QueueWorker to run")
    complete_callback = luigi.Parameter(default=None, description="Callback for when task completes")
    metadata = luigi.Parameter(default=None, description="Allows QueueSchedulers to attach data to a worker")

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


class TaskDescriptor(object):
    def __init__(self, task_name, task_args, metadata, module_name=None):
        self.task_name = task_name
        self.task_args = task_args
        self.metadata = metadata
        self.module_name = module_name


class QueueScheduler(luigi.WrapperTask):
    ''' Luigi 'daemon' for scheduling Tasks. Should return the same tasks on repeated invocation
        of fetch_queue_tasks, as long as they haven't been completed.
    '''
    def fetch_queue_tasks(self):
        ''' Return a list/generator of TaskDescriptors
        '''
        raise NotImplementedError

    def worker_complete(self, worker, success):
        ''' Will be called when a worker completes. Used to remove a task from the queue
        '''
        pass

    @property
    def task_deserializer(self):
        ''' Return a TaskDeserializer
        '''
        return ArgParseTaskDeserializer()

    def requires(self):
        tasks = list(self.fetch_queue_tasks())
        required = []
        for task in tasks:
            worker = QueueWorker(task=None, complete_callback=self.worker_complete, metadata=task.metadata)
            try:
                worker.task = self.task_deserializer.deserialize(task.task_name, task.task_name, task.module_name)
                required.append(worker)
            except DeserializerException:
                worker.complete_callback(worker, success=False)

        return required


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
                module_name = work_json.get('module_name')
                yield TaskDescriptor(
                    work_json['task_name'],
                    work_json['task_args'],
                    metadata=filename,
                    module_name=module_name)

    def worker_complete(self, worker, success):
        if not self.done_directory:
            self.done_directory = os.path.join(self.work_directory, "done")
        if not self.failed_directory:
            self.failed_directory = os.path.join(self.work_directory, "failed")

        if success:
            luigi.hdfs.rename(worker.metadata, self.done_directory)
        else:
            luigi.hdfs.rename(worker.metadata, self.failed_directory)
