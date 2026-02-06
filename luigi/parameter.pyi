# mypy: disable-error-code="misc"
"""
Type stub file for luigi.parameter

This stub file provides type hints to improve type inference when using Luigi parameters.
The primary goal is to enable correct type checking for Task definitions like:

    class MyTask(luigi.Task):
        s: str = luigi.Parameter()
        i: int = luigi.IntParameter()
        e: MyEnum = luigi.EnumParameter(enum=MyEnum)

Maintaining this stub file:
1. Run stubtest to validate against implementation:
   uv run python -m mypy.stubtest luigi.parameter --allowlist test/stubtest-allowlist.txt

2. If you add new Parameter classes or modify existing ones:
   - Add the corresponding stub definitions
   - Update class variables (like expected_type, date_format, etc.) if they're user-facing
   - Re-run stubtest and update test/stubtest-allowlist.txt if needed

3. The allowlist contains intentional differences:
   - Internal methods (parse, serialize, normalize) not included in stubs
   - __init__ signature differences (stubs use __new__ for type inference)
   - TypeVars that exist only in stubs for type inference

4. CI runs stubtest on every PR to ensure consistency
"""
from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, Callable, Generic, Optional, Type, Union, List, Tuple, Dict, TypedDict, overload
from typing_extensions import TypeVar, Unpack
import datetime

from luigi import date_interval
from luigi.freezing import FrozenOrderedDict
import luigi.task


class ParameterVisibility(IntEnum):
    """
    Possible values for the parameter visibility option. Public is the default.
    See :doc:`/parameters` for more info.
    """
    PUBLIC = 0
    HIDDEN = 1
    PRIVATE = 2

    @classmethod
    def has_value(cls, value: int) -> bool: ...

    def serialize(self) -> int: ...

class ParameterException(Exception): ...

class MissingParameterException(ParameterException): ...

class UnknownParameterException(ParameterException): ...

class DuplicateParameterException(ParameterException): ...

class OptionalParameterTypeWarning(UserWarning): ...

class UnconsumedParameterWarning(UserWarning): ...


T = TypeVar("T", default=str)

# TypedDict for common parameter kwargs
class _BaseParameterKwargs(TypedDict, Generic[T], total=False):
    default: Optional[T]
    is_global: bool
    significant: bool
    description: Optional[str]
    config_path: Optional[Dict[str, str]]
    positional: bool
    always_in_help: bool
    batch_method: Optional[Callable[[Any], Any]]
    visibility: Optional[int]

# Parameter inherits from str to allow s: str = Parameter()
class Parameter(Generic[T]):
    def __new__(
        cls,
        **kwargs: Unpack[_BaseParameterKwargs[T]]
    ) -> T: ...

    @overload
    def __get__(self, instance: None, owner: Any) -> "Parameter[T]": ...

    @overload
    def __get__(self, instance: Any, owner: Any) -> T: ...

    def __get__(self, instance: Any, owner: Any) -> Any: ...

    def has_task_value(self, task_name: str, param_name: str) -> bool: ...

    def task_value(self, task_name: str, param_name: str) -> T: ...

    def parse(self, x: Any) -> T: ...

    def serialize(self, x: T) -> str: ...

    def normalize(self, x: Any) -> T: ...

    def next_in_enumeration(self, value: T) -> Optional[T]: ...

class _OptionalParameterBase(Parameter[Union[T, None]]): ...

class OptionalParameter(_OptionalParameterBase[str]):
    expected_type: type[str]

class OptionalStrParameter(_OptionalParameterBase[str]):
    expected_type: type[str]

class DateParameter(Parameter[datetime.date]):
    date_format: str

class MonthParameter(DateParameter):
    date_format: str

class YearParameter(DateParameter):
    date_format: str

# TypedDict for DateHour/DateMinute/DateSecond parameter kwargs
class _DateTimeParameterKwargs(_BaseParameterKwargs[datetime.datetime], total=False):
    interval: int
    start: Optional[datetime.datetime]

class DateHourParameter(Parameter[datetime.datetime]):
    date_format: str
    interval: int
    start: datetime.datetime | None
    def __new__(
        cls,
        **kwargs: Unpack[_DateTimeParameterKwargs]
    ) -> datetime.datetime: ...

class DateMinuteParameter(Parameter[datetime.datetime]):
    date_format: str
    deprecated_date_format: str
    interval: int
    start: datetime.datetime | None
    def __new__(
        cls,
        **kwargs: Unpack[_DateTimeParameterKwargs]
    ) -> datetime.datetime: ...

class DateSecondParameter(Parameter[datetime.datetime]):
    date_format: str
    interval: int
    start: datetime.datetime | None
    def __new__(
        cls,
        **kwargs: Unpack[_DateTimeParameterKwargs]
    ) -> datetime.datetime: ...

class IntParameter(Parameter[int]): ...

class OptionalIntParameter(_OptionalParameterBase[int]):
    expected_type: type[int]

class FloatParameter(Parameter[float]): ...

class OptionalFloatParameter(_OptionalParameterBase[float]):
    expected_type: type[float]

class BoolParameter(Parameter[bool]):
    IMPLICIT_PARSING: str
    EXPLICIT_PARSING: str
    parsing: str

class OptionalBoolParameter(_OptionalParameterBase[bool]):
    expected_type: type[bool]

class DateIntervalParameter(Parameter[date_interval.DateInterval]): ...

class TimeDeltaParameter(Parameter[datetime.timedelta]): ...

TaskType = TypeVar("TaskType", bound="luigi.task.Task")

class TaskParameter(Parameter[Type[TaskType]]): ...

EnumParameterType = TypeVar('EnumParameterType', bound=Enum)

class _EnumParameterKwargs(_BaseParameterKwargs[T], Generic[EnumParameterType, T], total=False):
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

class OptionalDictParameter(Parameter[Union[Dict[Any, Any], None]]):
    expected_type: type[FrozenOrderedDict]

class _ListParameterKwargs(_BaseParameterKwargs[Tuple[T, ...]], Generic[T], total=False):
    schema: Any

class ListParameter(Parameter[Tuple[T, ...]]):
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[T]]
    ) -> Tuple[T, ...]: ...

class OptionalListParameter(Parameter[Optional[Tuple[T, ...]]]):
    expected_type: type[tuple]
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[T]]
    ) -> Optional[Tuple[T, ...]]: ...

class TupleParameter(Parameter[Tuple[T, ...]]):
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[T]]
    ) -> Tuple[T, ...]: ...

class OptionalTupleParameter(Parameter[Optional[Tuple[T, ...]]]):
    expected_type: type[tuple]
    def __new__(
        cls,
        **kwargs: Unpack[_ListParameterKwargs[T]]
    ) -> Optional[Tuple[T, ...]]: ...

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

class _ChoiceParameterKwargs(_BaseParameterKwargs[T], Generic[T, ChoiceType], total=False):
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

class OptionalPathParameter(_OptionalParameterBase[Path]):
    expected_type: Tuple[type[str], type[Path]]
