################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################

import importlib
import inspect
import logging
import os
import sys
import zipfile
import zipimport
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Tuple, get_type_hints

from pydantic import BaseModel, PrivateAttr, model_serializer

from flink_agents.plan.utils import check_type_match

# Global cache for PythonFunction instances to avoid repeated creation
_PYTHON_FUNCTION_CACHE: Dict[Tuple[str, str], "PythonFunction"] = {}

# PemJa MULTI_THREAD interpreters share one CPython process. The coordinated agent
# operator is therefore deployed as the process's sole Python owner, and only one
# artifact may be active in that process at a time.
_ACTIVE_PYTHON_ARTIFACT: Tuple[str, Tuple[str, ...]] | None = None

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _is_function_cacheable(func: Callable) -> bool:
    """Check if a function is safe to cache.

    Returns False for functions that should not be cached due to:
    - Closures (functions that capture variables from outer scope)
    - Generator functions (functions that yield values)
    - Functions with mutable default arguments
    - Instance methods (which depend on instance state)

    Parameters
    ----------
    func : Callable
        The function to check

    Returns:
    -------
    bool
        True if the function is safe to cache, False otherwise
    """
    if func is None:
        return False

    if inspect.isgeneratorfunction(func):
        return False

    if inspect.iscoroutinefunction(func):
        return False

    if inspect.ismethod(func):
        return False

    if hasattr(func, "__closure__") and func.__closure__ is not None:
        return False

    # Check for mutable default arguments
    try:
        sig = inspect.signature(func)
        for param in sig.parameters.values():
            if param.default is not inspect.Parameter.empty:
                # Check if default is a mutable type
                if isinstance(param.default, list | dict | set):
                    return False

                if param.default is None:
                    try:
                        source = inspect.getsource(func)
                        param_name = param.name
                        if f"if {param_name} is None:" in source and any(
                            mutable_type in source
                            for mutable_type in [
                                "[]",
                                "{}",
                                "list()",
                                "dict()",
                                "set()",
                            ]
                        ):
                            return False
                    except (OSError, TypeError):
                        pass
    except (ValueError, TypeError):
        return False

    return True


def switch_python_artifact(artifact_path: str | None) -> None:
    """Replace the process's active immutable user artifact.

    The caller has quiesced the old plan, so this is the only code mutating
    process-wide import state. ``None`` retires the active artifact. A new
    archive is inspected before any mutation, so an invalid artifact leaves the
    current import state untouched.
    """
    global _ACTIVE_PYTHON_ARTIFACT

    new_artifact_path = _normalize_path(artifact_path)
    new_roots = _python_artifact_roots(new_artifact_path)
    previous_artifact_path = None
    previous_roots: Tuple[str, ...] = ()
    if _ACTIVE_PYTHON_ARTIFACT is not None:
        previous_artifact_path, previous_roots = _ACTIVE_PYTHON_ARTIFACT

    _purge_python_modules(set(previous_roots) | set(new_roots))
    for path in {previous_artifact_path, new_artifact_path}:
        if path is not None:
            _remove_python_artifact_path(path)
    if new_artifact_path is not None:
        sys.path.insert(0, new_artifact_path)

    _PYTHON_FUNCTION_CACHE.clear()
    _ACTIVE_PYTHON_ARTIFACT = (
        (new_artifact_path, new_roots) if new_artifact_path is not None else None
    )


def _python_artifact_roots(artifact_path: str | None) -> Tuple[str, ...]:
    """Return import roots owned by one directly importable zip/whl artifact."""
    if artifact_path is None:
        return ()

    roots = set()
    with zipfile.ZipFile(artifact_path) as archive:
        for entry in archive.infolist():
            if entry.is_dir() or not entry.filename.endswith(".py"):
                continue
            parts = entry.filename.split("/")
            if not parts or any(part in {"", ".", ".."} for part in parts):
                continue
            root = parts[0][:-3] if len(parts) == 1 else parts[0]
            if root.isidentifier() and not _is_framework_module(root):
                roots.add(root)
    return tuple(sorted(roots))


def _purge_python_modules(module_roots: Iterable[str]) -> None:
    owned_roots = set(module_roots)
    for loaded_name in list(sys.modules):
        if not _is_framework_module(loaded_name) and (
            loaded_name.split(".", 1)[0] in owned_roots
        ):
            sys.modules.pop(loaded_name, None)


def _remove_python_artifact_path(artifact_path: str) -> None:
    sys.path[:] = [
        entry
        for entry in sys.path
        if _normalize_path(entry) != artifact_path
    ]
    for cached_path in list(sys.path_importer_cache):
        if _path_belongs_to_artifact(cached_path, artifact_path):
            del sys.path_importer_cache[cached_path]
    for cached_path in list(zipimport._zip_directory_cache):
        if _path_belongs_to_artifact(cached_path, artifact_path):
            del zipimport._zip_directory_cache[cached_path]


def _path_belongs_to_artifact(candidate: Any, artifact_path: str) -> bool:
    normalized = _normalize_path(candidate)
    return normalized is not None and (
        normalized == artifact_path
        or normalized.startswith(artifact_path + os.sep)
    )


def _normalize_path(candidate: Any) -> str | None:
    if not isinstance(candidate, str) or not candidate.strip():
        return None
    try:
        return os.path.realpath(candidate)
    except (OSError, TypeError, ValueError):
        return None


def _is_framework_module(module: str) -> bool:
    return module == "flink_agents" or module.startswith("flink_agents.")


class Function(BaseModel, ABC):
    """Base interface for user-defined functions."""

    @abstractmethod
    def check_signature(self, *args: Tuple[Any, ...]) -> None:
        """Check function signature is legal or not."""

    @abstractmethod
    def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
        """Execute function."""


class PythonFunction(Function):
    """Descriptor for a python callable function, storing module and qualified name for
    dynamic retrieval.

    This class allows serialization and lazy loading of functions by storing their
    module and qualified name. The actual callable is loaded on-demand when the instance
    is called.

    Attributes:
    ----------
    module : str
        Name of the Python module where the function is defined.
    qualname : str
        Qualified name of the function (e.g., 'ClassName.method' for class methods).
    """

    module: str
    qualname: str
    __func: Callable = None
    __is_cacheable: bool = None

    @model_serializer
    def __custom_serializer(self) -> dict[str, Any]:
        data = {
            "func_type": self.__class__.__qualname__,
            "module": self.module,
            "qualname": self.qualname,
        }
        return data

    @staticmethod
    def from_callable(func: Callable) -> "PythonFunction":
        """Create a Function descriptor from an existing callable.

        Parameters
        ----------
        func : Callable
            The function or method to be wrapped.

        Returns:
        -------
        Function
            A Function instance with module and qualname populated based on the input
            callable.
        """
        return PythonFunction(
            module=inspect.getmodule(func).__name__,
            qualname=func.__qualname__,
            __func=func,
        )

    def check_signature(self, *args: Tuple[Any, ...]) -> None:
        """Check function signature."""
        func = self.__get_func()
        params = inspect.signature(func).parameters

        # Use get_type_hints to resolve string annotations properly
        try:
            type_hints = get_type_hints(func)
            annotations = [
                type_hints.get(param_name, param.annotation)
                for param_name, param in params.items()
            ]
        except (NameError, AttributeError):
            # Fallback to raw annotations if get_type_hints fails
            annotations = [param.annotation for param in params.values()]

        err_msg = (
            f"Expect {self.qualname} have signature {args}, but got {annotations}."
        )
        if len(params) != len(args):
            raise TypeError(err_msg)
        try:
            for i, annotation in enumerate(annotations):
                check_type_match(annotation, args[i])
        except TypeError as e:
            raise TypeError(err_msg) from e

    def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
        """Execute the stored function with provided arguments.

        Lazily loads the function from its module and qualified name if not already
        cached.

        Parameters
        ----------
        *args : tuple
            Positional arguments to pass to the function.
        **kwargs : dict
            Keyword arguments to pass to the function.

        Returns:
        -------
        Any
            The result of calling the resolved function with the provided arguments.

        Notes:
        -----
        If the function is a method (qualified name contains a class reference), it will
        resolve the method from the corresponding class.
        """
        return self.__get_func()(*args, **kwargs)

    def as_callable(self) -> Callable:
        """Return the underlying Python callable, importing the module if needed."""
        return self.__get_func()

    def __get_func(self) -> Callable:
        if self.__func is None:
            module = importlib.import_module(self.module)
            # TODO: support function of inner class.
            if "." in self.qualname:
                # Handle class methods (e.g., 'ClassName.method')
                classname, methodname = self.qualname.rsplit(".", 1)
                clazz = getattr(module, classname)
                self.__func = getattr(clazz, methodname)
            else:
                # Handle standalone functions
                self.__func = getattr(module, self.qualname)
        return self.__func

    def is_cacheable(self) -> bool:
        """Check if this function is cacheable, caching the result for future calls."""
        if self.__is_cacheable is None:
            self.__is_cacheable = _is_function_cacheable(self.__get_func())
        return self.__is_cacheable


class JavaFunction(Function):
    """Descriptor for a Java callable function.

    Invocation goes through the JVM resource adapter, injected by the
    runtime via :meth:`set_java_resource_adapter`; until then
    ``__call__`` raises ``RuntimeError``.
    """

    qualname: str
    method_name: str
    parameter_types: List[str]

    _j_resource_adapter: Any = PrivateAttr(default=None)

    @model_serializer
    def __custom_serializer(self) -> dict[str, Any]:
        data = {
            "func_type": self.__class__.__qualname__,
            "qualname": self.qualname,
            "method_name": self.method_name,
            "parameter_types": self.parameter_types,
        }
        return data

    def set_java_resource_adapter(self, adapter: Any) -> None:
        """Inject the JVM adapter used to invoke this Java method."""
        self._j_resource_adapter = adapter

    def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
        """Invoke the Java method via the JVM resource adapter.

        Positional args route to ``invokeJavaAction`` (action dispatch);
        keyword args route to ``invokeJavaTool`` (LLM tool dispatch).
        """
        if self._j_resource_adapter is None:
            msg = (
                "JavaFunction requires the JVM resource adapter; not set "
                "on this descriptor. The runtime injects it via "
                "set_java_resource_adapter before invocation."
            )
            raise RuntimeError(msg)
        if args and kwargs:
            msg = (
                "JavaFunction does not support mixing positional and keyword "
                "args; pass one or the other (positional = action, kwargs = tool)."
            )
            raise TypeError(msg)
        if args:
            return self._j_resource_adapter.invokeJavaAction(
                self.qualname,
                self.method_name,
                self.parameter_types,
                list(args),
            )
        return self._j_resource_adapter.invokeJavaTool(
            self.qualname,
            self.method_name,
            self.parameter_types,
            kwargs,
        )

    def check_signature(self, *args: Tuple[Any, ...]) -> None:
        """Check declared Java parameter arity matches expectations."""
        if len(self.parameter_types) != len(args):
            msg = (
                f"JavaFunction {self.qualname}.{self.method_name} declares "
                f"{len(self.parameter_types)} parameter type(s) "
                f"({self.parameter_types!r}) but the action contract "
                f"expects {len(args)}."
            )
            raise TypeError(msg)


def resolve_python_function(module: str, qualname: str) -> None:
    """Import and resolve one declared action without executing it."""
    python_func = PythonFunction(module=module, qualname=qualname)
    python_func.as_callable()


def call_python_function(
    module: str, qualname: str, func_args: Tuple[Any, ...]
) -> Any:
    """Used to call a Python function in the Pemja environment.

    Uses selective caching to reuse PythonFunction instances for identical
    (module, qualname) pairs to improve performance during frequent invocations.
    Artifact switching clears this cache at the checkpoint barrier before the
    new plan resolves any action.
    Only caches functions that are safe to cache (no closures, generators, etc.).

    Parameters
    ----------
    module : str
        Name of the Python module where the function is defined.
    qualname : str
        Qualified name of the function (e.g., 'ClassName.method' for class methods).
    func_args : Tuple[Any, ...]
        Arguments to pass to the function.

    Returns:
    -------
    Any
        The result of calling the function with the provided arguments.
    """
    cache_key = (module, qualname)

    python_func = None

    if cache_key not in _PYTHON_FUNCTION_CACHE:
        python_func = PythonFunction(module=module, qualname=qualname)
        if python_func.is_cacheable():
            _PYTHON_FUNCTION_CACHE[cache_key] = python_func
    else:
        python_func = _PYTHON_FUNCTION_CACHE[cache_key]

    func_result = python_func(*func_args)
    return func_result


def clear_python_function_cache() -> None:
    """Clear the PythonFunction cache.

    This function is useful for testing or when you want to ensure
    fresh function instances are created.
    """
    global _PYTHON_FUNCTION_CACHE
    _PYTHON_FUNCTION_CACHE.clear()


def get_python_function_cache_size() -> int:
    """Get the current size of the PythonFunction cache.

    Returns:
    -------
    int
        The number of cached PythonFunction instances.
    """
    return len(_PYTHON_FUNCTION_CACHE)


def get_python_function_cache_keys() -> List[Tuple[str, str]]:
    """Get all cache keys (module, qualname) pairs currently in the cache.

    Returns:
    -------
    List[Tuple[str, str]]
        List of (module, qualname) tuples representing cached functions.
    """
    return list(_PYTHON_FUNCTION_CACHE.keys())


_ASYNCIO_ERROR_MESSAGE = (
    "asyncio functions (gather/wait/create_task/sleep) are not supported "
    "in Flink Agents. Only 'await ctx.durable_execute_async(...)' is supported."
)


def call_python_awaitable(awaitable: Any) -> Tuple[bool, Any]:
    """Invokes the next step of a Python coroutine or generator and returns whether
    it is done, along with the yielded or returned value.

    Args:
        awaitable: A Python coroutine or generator object that can be driven
        by the send() method.

    Returns:
        Tuple[bool, Any]:
            - The first element is a boolean flag indicating whether the awaitable
            has finished:
                * False: The awaitable has more values to yield.
                * True: The awaitable has completed.
            - The second element is either:
                * The value yielded by the awaitable (when not exhausted), or
                * The return value of the awaitable (when it has finished).
    """
    try:
        result = awaitable.send(None)
    except StopIteration as e:
        return True, e.value if hasattr(e, "value") else None
    except RuntimeError as e:
        err_msg = str(e)
        if (
            "no running event loop" in err_msg
            or "await wasn't used with future" in err_msg
        ):
            raise RuntimeError(_ASYNCIO_ERROR_MESSAGE) from e
        raise
    except Exception:
        logger.exception("Error in awaitable execution")
        raise
    else:
        return False, result
