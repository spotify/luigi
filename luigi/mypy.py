"""Plugin that provides support for luigi.Task

This Code reuses the code from mypy.plugins.dataclasses
https://github.com/python/mypy/blob/0753e2a82dad35034e000609b6e8daa37238bfaa/mypy/plugins/dataclasses.py
"""

from __future__ import annotations

import sys
from typing import Callable, Dict, Final, Iterator, List, Literal, Optional

from mypy.expandtype import expand_type, expand_type_by_instance
from mypy.nodes import (
    ARG_NAMED_OPT,
    ARG_POS,
    Argument,
    AssignmentStmt,
    Block,
    CallExpr,
    ClassDef,
    Context,
    EllipsisExpr,
    Expression,
    FuncDef,
    IfStmt,
    JsonDict,
    MemberExpr,
    NameExpr,
    PlaceholderNode,
    RefExpr,
    Statement,
    SymbolTableNode,
    TempNode,
    TypeInfo,
    Var,
)
from mypy.plugin import (
    ClassDefContext,
    FunctionContext,
    Plugin,
    SemanticAnalyzerPluginInterface,
)
from mypy.plugins.common import (
    add_method_to_class,
    deserialize_and_fixup_type,
)
from mypy.server.trigger import make_wildcard_trigger
from mypy.state import state
from mypy.typeops import map_type_from_supertype
from mypy.types import (
    AnyType,
    CallableType,
    Instance,
    NoneType,
    Type,
    TypeOfAny,
    get_proper_type,
)
from mypy.typevars import fill_typevars

METADATA_TAG: Final[str] = "task"

if sys.version_info[:2] < (3, 8):
    # This plugin uses the walrus operator, which is only available in Python 3.8+
    raise RuntimeError("This plugin requires Python 3.8+")


class TaskPlugin(Plugin):
    def get_base_class_hook(
        self, fullname: str
    ) -> Callable[[ClassDefContext], None] | None:
        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):
            if any(base.fullname == "luigi.task.Task" for base in sym.node.mro):
                return self._task_class_maker_callback
        return None

    def get_function_hook(
        self, fullname: str
    ) -> Callable[[FunctionContext], Type] | None:
        """Adjust the return type of the `Parameters` function."""
        if self.check_parameter(fullname):
            return self._task_parameter_field_callback
        return None

    def check_parameter(self, fullname):
        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):
            return any(base.fullname == "luigi.parameter.Parameter" for base in sym.node.mro)

    def _task_class_maker_callback(self, ctx: ClassDefContext) -> None:
        transformer = TaskTransformer(ctx.cls, ctx.reason, ctx.api, self)
        transformer.transform()

    def _task_parameter_field_callback(self, ctx: FunctionContext) -> Type:
        """Extract the type of the `default` argument from the Field function, and use it as the return type.

        In particular:
        * Retrieve the type of the argument which is specified, and use it as return type for the function.
        * If no default argument is specified, return AnyType with unannotated type instead of parameter types like `luigi.Parameter()`
          This makes mypy avoid conflict between the type annotation and the parameter type.
          e.g.
          ```python
          foo: int = luigi.IntParameter()
          ```
        """
        try:
            default_idx = ctx.callee_arg_names.index("default")
        # if no `default` argument is found, return AnyType with unannotated type.
        except ValueError:
            return AnyType(TypeOfAny.unannotated)

        default_args = ctx.args[default_idx]

        if default_args:
            default_type = ctx.arg_types[0][0]
            default_arg = default_args[0]

            # Fallback to default Any type if the field is required
            if not isinstance(default_arg, EllipsisExpr):
                return default_type
        # NOTE: This is a workaround to avoid the error between type annotation and parameter type.
        #       As the following code snippet, the type of `foo` is `int` but the assigned value is `luigi.IntParameter()`.
        #       foo: int = luigi.IntParameter()
        # TODO: infer mypy type from the parameter type.
        return AnyType(TypeOfAny.unannotated)


class TaskAttribute:
    def __init__(
        self,
        name: str,
        has_default: bool,
        line: int,
        column: int,
        type: Type | None,
        info: TypeInfo,
        api: SemanticAnalyzerPluginInterface,
    ) -> None:
        self.name = name
        self.has_default = has_default
        self.line = line
        self.column = column
        self.type = type  # Type as __init__ argument
        self.info = info
        self._api = api

    def to_argument(
        self, current_info: TypeInfo, *, of: Literal["__init__",]
    ) -> Argument:
        if of == "__init__":
            # All arguments to __init__ are keyword-only and optional
            # This is because gokart can set parameters by configuration'
            arg_kind = ARG_NAMED_OPT
        return Argument(
            variable=self.to_var(current_info),
            type_annotation=self.expand_type(current_info),
            initializer=EllipsisExpr()
            if self.has_default
            else None,  # Only used by stubgen
            kind=arg_kind,
        )

    def expand_type(self, current_info: TypeInfo) -> Type | None:
        if self.type is not None and self.info.self_type is not None:
            # In general, it is not safe to call `expand_type()` during semantic analysis,
            # however this plugin is called very late, so all types should be fully ready.
            # Also, it is tricky to avoid eager expansion of Self types here (e.g. because
            # we serialize attributes).
            with state.strict_optional_set(self._api.options.strict_optional):
                return expand_type(
                    self.type, {self.info.self_type.id: fill_typevars(current_info)}
                )
        return self.type

    def to_var(self, current_info: TypeInfo) -> Var:
        return Var(self.name, self.expand_type(current_info))

    def serialize(self) -> JsonDict:
        assert self.type
        return {
            "name": self.name,
            "has_default": self.has_default,
            "line": self.line,
            "column": self.column,
            "type": self.type.serialize(),
        }

    @classmethod
    def deserialize(
        cls, info: TypeInfo, data: JsonDict, api: SemanticAnalyzerPluginInterface
    ) -> TaskAttribute:
        data = data.copy()
        typ = deserialize_and_fixup_type(data.pop("type"), api)
        return cls(type=typ, info=info, **data, api=api)

    def expand_typevar_from_subtype(self, sub_type: TypeInfo) -> None:
        """Expands type vars in the context of a subtype when an attribute is inherited
        from a generic super type."""
        if self.type is not None:
            with state.strict_optional_set(self._api.options.strict_optional):
                self.type = map_type_from_supertype(self.type, sub_type, self.info)


class TaskTransformer:
    """Implement the behavior of gokart.Task."""

    def __init__(
        self,
        cls: ClassDef,
        reason: Expression | Statement,
        api: SemanticAnalyzerPluginInterface,
        task_plugin: TaskPlugin,
    ) -> None:
        self._cls = cls
        self._reason = reason
        self._api = api
        self._task_plugin = task_plugin

    def transform(self) -> bool:
        """Apply all the necessary transformations to the underlying gokart.Task"""
        info = self._cls.info
        attributes = self.collect_attributes()

        if attributes is None:
            # Some definitions are not ready. We need another pass.
            return False
        for attr in attributes:
            if attr.type is None:
                return False
        # If there are no attributes, it may be that the semantic analyzer has not
        # processed them yet. In order to work around this, we can simply skip generating
        # __init__ if there are no attributes, because if the user truly did not define any,
        # then the object default __init__ with an empty signature will be present anyway.
        if (
            "__init__" not in info.names or info.names["__init__"].plugin_generated
        ) and attributes:
            args = [attr.to_argument(info, of="__init__") for attr in attributes]
            add_method_to_class(
                self._api, self._cls, "__init__", args=args, return_type=NoneType()
            )
        info.metadata[METADATA_TAG] = {
            "attributes": [attr.serialize() for attr in attributes],
        }

        return True

    def _get_assignment_statements_from_if_statement(
        self, stmt: IfStmt
    ) -> Iterator[AssignmentStmt]:
        for body in stmt.body:
            if not body.is_unreachable:
                yield from self._get_assignment_statements_from_block(body)
        if stmt.else_body is not None and not stmt.else_body.is_unreachable:
            yield from self._get_assignment_statements_from_block(stmt.else_body)

    def _get_assignment_statements_from_block(
        self, block: Block
    ) -> Iterator[AssignmentStmt]:
        for stmt in block.body:
            if isinstance(stmt, AssignmentStmt):
                yield stmt
            elif isinstance(stmt, IfStmt):
                yield from self._get_assignment_statements_from_if_statement(stmt)

    def collect_attributes(self) -> Optional[List[TaskAttribute]]:
        """Collect all attributes declared in the task and its parents.

        All assignments of the form

          a: SomeType
          b: SomeOtherType = ...

        are collected.

        Return None if some base class hasn't been processed
        yet and thus we'll need to ask for another pass.
        """
        cls = self._cls

        # First, collect attributes belonging to any class in the MRO, ignoring duplicates.
        #
        # We iterate through the MRO in reverse because attrs defined in the parent must appear
        # earlier in the attributes list than attrs defined in the child.
        #
        # However, we also want attributes defined in the subtype to override ones defined
        # in the parent. We can implement this via a dict without disrupting the attr order
        # because dicts preserve insertion order in Python 3.7+.
        found_attrs: Dict[str, TaskAttribute] = {}
        for info in reversed(cls.info.mro[1:-1]):
            if METADATA_TAG not in info.metadata:
                continue
            # Each class depends on the set of attributes in its task ancestors.
            self._api.add_plugin_dependency(make_wildcard_trigger(info.fullname))

            for data in info.metadata[METADATA_TAG]["attributes"]:
                name: str = data["name"]

                attr = TaskAttribute.deserialize(info, data, self._api)
                # TODO: We shouldn't be performing type operations during the main
                #       semantic analysis pass, since some TypeInfo attributes might
                #       still be in flux. This should be performed in a later phase.
                attr.expand_typevar_from_subtype(cls.info)
                found_attrs[name] = attr

                sym_node = cls.info.names.get(name)
                if sym_node and sym_node.node and not isinstance(sym_node.node, Var):
                    self._api.fail(
                        "Task attribute may only be overridden by another attribute",
                        sym_node.node,
                    )

        # Second, collect attributes belonging to the current class.
        current_attr_names: set[str] = set()
        for stmt in self._get_assignment_statements_from_block(cls.defs):
            if not self.is_parameter_call(stmt.rvalue):
                continue

            # a: int, b: str = 1, 'foo' is not supported syntax so we
            # don't have to worry about it.
            lhs = stmt.lvalues[0]
            if not isinstance(lhs, NameExpr):
                continue
            sym = cls.info.names.get(lhs.name)
            if sym is None:
                # There was probably a semantic analysis error.
                continue

            node = sym.node
            assert not isinstance(node, PlaceholderNode)

            assert isinstance(node, Var)

            has_parameter_call, parameter_args = self._collect_parameter_args(
                stmt.rvalue
            )
            has_default = False
            # Ensure that something like x: int = field() is rejected
            # after an attribute with a default.
            if has_parameter_call:
                has_default = "default" in parameter_args

            # All other assignments are already type checked.
            elif not isinstance(stmt.rvalue, TempNode):
                has_default = True

            if not has_default:
                # Make all non-default task attributes implicit because they are de-facto
                # set on self in the generated __init__(), not in the class body. On the other
                # hand, we don't know how custom task transforms initialize attributes,
                # so we don't treat them as implicit. This is required to support descriptors
                # (https://github.com/python/mypy/issues/14868).
                sym.implicit = True

            current_attr_names.add(lhs.name)
            with state.strict_optional_set(self._api.options.strict_optional):
                init_type = self._infer_task_attr_init_type(sym, stmt)

            found_attrs[lhs.name] = TaskAttribute(
                name=lhs.name,
                has_default=has_default,
                line=stmt.line,
                column=stmt.column,
                type=init_type,
                info=cls.info,
                api=self._api,
            )

        return list(found_attrs.values())

    def _collect_parameter_args(
        self, expr: Expression
    ) -> tuple[bool, Dict[str, Expression]]:
        """Returns a tuple where the first value represents whether or not
        the expression is a call to luigi.Parameter()
        and the second value is a dictionary of the keyword arguments that luigi.Parameter() was called with.
        """
        if isinstance(expr, CallExpr) and isinstance(expr.callee, RefExpr):
            args = {}
            for name, arg in zip(expr.arg_names, expr.args):
                if name is None:
                    # NOTE: this is a workaround to get default value from a parameter
                    self._api.fail(
                        "Positional arguments are not allowed for parameters when using the mypy plugin. "
                        "Update your code to use named arguments, like luigi.Parameter(default='foo') instead of luigi.Parameter('foo')",
                        expr,
                    )
                    continue
                args[name] = arg
            return True, args
        return False, {}

    def _infer_task_attr_init_type(
        self, sym: SymbolTableNode, context: Context
    ) -> Type | None:
        """Infer __init__ argument type for an attribute.

        In particular, possibly use the signature of __set__.
        """
        default = sym.type
        if sym.implicit:
            return default
        t = get_proper_type(sym.type)

        # Perform a simple-minded inference from the signature of __set__, if present.
        # We can't use mypy.checkmember here, since this plugin runs before type checking.
        # We only support some basic scanerios here, which is hopefully sufficient for
        # the vast majority of use cases.
        if not isinstance(t, Instance):
            return default
        setter = t.type.get("__set__")

        if not setter:
            return default

        if isinstance(setter.node, FuncDef):
            super_info = t.type.get_containing_type_info("__set__")
            assert super_info
            if setter.type:
                setter_type = get_proper_type(
                    map_type_from_supertype(setter.type, t.type, super_info)
                )
            else:
                return AnyType(TypeOfAny.unannotated)
            if isinstance(setter_type, CallableType) and setter_type.arg_kinds == [
                ARG_POS,
                ARG_POS,
                ARG_POS,
            ]:
                return expand_type_by_instance(setter_type.arg_types[2], t)
            else:
                self._api.fail(
                    f'Unsupported signature for "__set__" in "{t.type.name}"', context
                )
        else:
            self._api.fail(f'Unsupported "__set__" in "{t.type.name}"', context)

        return default

    def is_parameter_call(self, expr: Expression) -> bool:
        """Checks if the expression is a call to luigi.Parameter()"""
        if not isinstance(expr, CallExpr):
            return False

        callee = expr.callee
        fullname = None
        if isinstance(callee, MemberExpr):
            type_info = callee.node
            if type_info is None and isinstance(callee.expr, NameExpr):
                fullname = f"{callee.expr.name}.{callee.name}"
        elif isinstance(callee, NameExpr):
            type_info = callee.node
        else:
            return False

        if isinstance(type_info, TypeInfo):
            fullname = type_info.fullname

        return fullname is not None and self._task_plugin.check_parameter(fullname)


def plugin(version: str) -> type[Plugin]:
    return TaskPlugin
