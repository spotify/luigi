import task
import file  # wtf @ naming
import rpc
import parameter
import interface
import target

Task = task.Task
ExternalTask = task.ExternalTask
WrapperTask = task.WrapperTask
Target = target.Target

File = file.File  # TODO: remove, should be LocalTarget
LocalTarget = File
Parameter = parameter.Parameter
RemoteScheduler = rpc.RemoteScheduler

expose = interface.expose
expose_main = interface.expose_main
run = interface.run

# TODO: how can we get rid of these?
DateHourParameter = parameter.DateHourParameter
DateParameter = parameter.DateParameter
IntParameter = parameter.IntParameter
BooleanParameter = parameter.BooleanParameter
DateIntervalParameter = parameter.DateIntervalParameter

namespace = task.namespace
