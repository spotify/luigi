from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, Callable, Generic, Optional, Type, Union, List, Tuple, Dict, TypedDict
from typing_extensions import TypeVar, Unpack
import datetime

from luigi import date_interval
import luigi.task


class ParameterVisibility(IntEnum):
    """
    Possible values for the parameter visibility option. Public is the default.
    See :doc:`/parameters` for more info.
    """
    PUBLIC = 0
    HIDDEN = 1
    PRIVATE = 2

class ParameterException(Exception): ...

class MissingParameterException(ParameterException): ...

class UnknownParameterException(ParameterException): ...

class DuplicateParameterException(ParameterException): ...

class OptionalParameterTypeWarning(UserWarning): ...

class UnconsumedParameterWarning(UserWarning): ...


_T = TypeVar("_T", default=str)

# TypedDict for common parameter kwargs
class _BaseParameterKwargs(TypedDict, Generic[_T], total=False):
    default: Optional[_T]
    is_global: bool
    significant: bool
    description: Optional[str]
    config_path: Optional[Dict[str, str]]
    positional: bool
    always_in_help: bool
    batch_method: Optional[Callable[[Any], Any]]
    visibility: Optional[int]

# Parameter inherits from str to allow s: str = Parameter()
class Parameter(Generic[_T]):
    def __new__(
        cls,
        **kwargs: Unpack[_BaseParameterKwargs[_T]]
    ) -> _T: ...

class _OptionalParameterBase(Parameter[Union[_T, None]]): ...

class OptionalParameter(_OptionalParameterBase[str]): ...

class OptionalStrParameter(_OptionalParameterBase[str]): ...

class DateParameter(Parameter[datetime.date]): ...

class MonthParameter(DateParameter): ...

class YearParameter(DateParameter): ...

class DateHourParameter(Parameter[datetime.datetime]): ...

class DateMinuteParameter(Parameter[datetime.datetime]): ...

class DateSecondParameter(Parameter[datetime.datetime]): ...

class IntParameter(Parameter[int]): ...

class OptionalIntParameter(_OptionalParameterBase[int]): ...

class FloatParameter(Parameter[float]): ...

class OptionalFloatParameter(_OptionalParameterBase[float]): ...

class BoolParameter(Parameter[bool]): ...

class OptionalBoolParameter(_OptionalParameterBase[bool]): ...

class DateIntervalParameter(Parameter[date_interval.DateInterval]): ...

class TimeDeltaParameter(Parameter[datetime.timedelta]): ...

TaskType = TypeVar("TaskType", bound="luigi.task.Task")

class TaskParameter(Parameter[Type[TaskType]]): ...

EnumParameterType = TypeVar('EnumParameterType', bound=Enum)

class _EnumParameterKwargs(_BaseParameterKwargs[_T], Generic[EnumParameterType, _T], total=False):
    enum: Type[EnumParameterType]

class EnumParameter(Parameter[EnumParameterType]):
    def __new__(
        cls,
        **kwargs: Unpack[_EnumParameterKwargs[EnumParameterType, EnumParameterType]],
    ) -> EnumParameterType: ...

class EnumListParameter(Parameter[Tuple[EnumParameterType, ...]]):
    def __new__(
        cls,
        **kwargs: Unpack[_EnumParameterKwargs[EnumParameterType, Tuple[EnumParameterType, ...]]],
    ) -> Tuple[EnumParameterType, ...]: ...

class DictParameter(Parameter[Dict[Any, Any]]): ...

class OptionalDictParameter(Parameter[Union[Dict[Any, Any], None]]): ...

class _ListParameterKwargs(_BaseParameterKwargs[Tuple[_T, ...]], Generic[_T], total=False):
    schema: Any

class ListParameter(Parameter[Tuple[_T, ...]]):
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[_T]]
    ) -> Tuple[_T, ...]: ...

class OptionalListParameter(Parameter[Optional[Tuple[_T, ...]]]):
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[_T]]
    ) -> Optional[Tuple[_T, ...]]: ...

class TupleParameter(Parameter[Tuple[_T, ...]]):
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[_T]]
    ) -> Tuple[_T, ...]: ...

class OptionalTupleParameter(Parameter[Optional[Tuple[_T, ...]]]):
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[_T]]
    ) -> Optional[Tuple[_T, ...]]: ...

NumericalType = TypeVar("NumericalType", int, float)

# TypedDict for NumericalParameter-specific kwargs
class _NumericalParameterKwargs(_BaseParameterKwargs[NumericalType], Generic[NumericalType], total=False):
    var_type: Optional[Type[NumericalType]]
    min_value: Optional[NumericalType]
    max_value: Optional[NumericalType]
    left_op: Callable[[NumericalType, NumericalType], bool]
    right_op: Callable[[NumericalType, NumericalType], bool]

class NumericalParameter(Parameter[NumericalType]):
    def __new__(
        cls,
        **kwargs: Unpack[_NumericalParameterKwargs[NumericalType]]
    ) -> NumericalType: ...

class OptionalNumericalParameter(_OptionalParameterBase[NumericalType]):
    def __new__(
        cls,
        **kwargs: Unpack[_NumericalParameterKwargs[NumericalType]]
    ) -> Optional[NumericalType]: ...

ChoiceType = TypeVar("ChoiceType", default=str)

class _ChoiceParameterKwargs(_BaseParameterKwargs[_T], Generic[_T, ChoiceType], total=False):
    choices: List[ChoiceType]
    var_type: Type[ChoiceType]

class ChoiceParameter(Parameter[ChoiceType]):
    def __new__(
        cls,
        **kwargs: Unpack[_ChoiceParameterKwargs[ChoiceType, ChoiceType]],
    ) -> ChoiceType: ...

class ChoiceListParameter(Parameter[Tuple[ChoiceType, ...]]):
    def __new__(
        cls,
        **kwargs: Unpack[_ChoiceParameterKwargs[Tuple[ChoiceType, ...], ChoiceType]],
    ) -> Tuple[ChoiceType, ...]: ...

class OptionalChoiceParameter(_OptionalParameterBase[ChoiceType]):
    def __new__(
        cls,
        **kwargs: Unpack[_ChoiceParameterKwargs[Optional[ChoiceType], ChoiceType]],
    ) -> Optional[ChoiceType]: ...

class PathParameter(Parameter[Path]): ...
