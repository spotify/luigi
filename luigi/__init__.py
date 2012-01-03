import task, file, scheduler, parameter, interface, target

Task = task.Task
ExternalTask = task.ExternalTask

File = file.File
LocalTarget = File # Can't decide what we should call it...
Parameter = parameter.Parameter
RemoteScheduler = scheduler.RemoteScheduler

expose = interface.expose
expose_main = interface.expose_main
run = interface.run

# TODO: how can we get rid of these?
DateParameter = parameter.DateParameter
IntParameter = parameter.IntParameter

# TODO: this is just ugly glue code right now, until we've fully ported the paths from builder2
try:
    from spotify.builder2.paths import HdfsPath, S3Path, DynamicPath
    
    class HdfsTarget(HdfsPath, target.Target): pass
    class S3Target(S3Path, target.Target): pass
    class DynamicTarget(DynamicPath, target.Target): pass
except ImportError:
    import warnings
    warnings.warn('No S3/HDFS functionality')
