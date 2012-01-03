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

