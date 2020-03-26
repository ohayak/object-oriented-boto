import logging
import os
from inspect import isclass
from functools import wraps
import autologging
from .jsonformatter import JsonFormatter

__all__ = ["TRACE", "logged", "traced", "lambdalogs"]

TRACE = autologging.TRACE


def traced(*args, **kwargs):
    """Add call and return tracing to an unbound function or to the
    methods of a class.

    The arguments to ``traced`` differ depending on whether it is being
    used to trace an unbound function or the methods of a class:

    .. rubric:: Trace an unbound function using the default logger

    :arg func: the unbound function to be traced

    By default, a logger named for the function's module is used:

    >>> import sys
    >>> logging.basicConfig(
    ...     level=TRACE, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @traced
    ... def func(x, y):
    ...     return x + y
    ...
    >>> func(7, 9)
    TRACE:autologging:func:CALL *(7, 9) **{}
    TRACE:autologging:func:RETURN 16
    16

    .. rubric:: Trace an unbound function using a named logger

    :arg logging.Logger logger:
       the parent logger used to trace the unbound function

    >>> import sys
    >>> logging.basicConfig(
    ...     level=TRACE, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @traced(logging.getLogger("my.channel"))
    ... def func(x, y):
    ...     return x + y
    ...
    >>> func(7, 9)
    TRACE:my.channel:func:CALL *(7, 9) **{}
    TRACE:my.channel:func:RETURN 16
    16

    .. rubric:: Trace default methods using the default logger

    :arg class_: the class whose methods will be traced

    By default, all "public", "_nonpublic", and "__internal" methods, as
    well as the special "__init__" and "__call__" methods, will be
    traced. Tracing log entries will be written to a logger named for
    the module and class:

    >>> import sys
    >>> logging.basicConfig(
    ...     level=TRACE, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @traced
    ... class Class:
    ...     def __init__(self, x):
    ...         self._x = x
    ...     def public(self, y):
    ...         return self._x + y
    ...     def _nonpublic(self, y):
    ...         return self._x - y
    ...     def __internal(self, y=2):
    ...         return self._x ** y
    ...     def __repr__(self):
    ...         return "Class(%r)" % self._x
    ...     def __call__(self):
    ...         return self._x
    ...
    >>> obj = Class(7)
    TRACE:autologging.Class:__init__:CALL *(7,) **{}
    >>> obj.public(9)
    TRACE:autologging.Class:public:CALL *(9,) **{}
    TRACE:autologging.Class:public:RETURN 16
    16
    >>> obj._nonpublic(5)
    TRACE:autologging.Class:_nonpublic:CALL *(5,) **{}
    TRACE:autologging.Class:_nonpublic:RETURN 2
    2
    >>> obj._Class__internal(y=3)
    TRACE:autologging.Class:__internal:CALL *() **{'y': 3}
    TRACE:autologging.Class:__internal:RETURN 343
    343
    >>> repr(obj) # not traced by default
    'Class(7)'
    >>> obj()
    TRACE:autologging.Class:__call__:CALL *() **{}
    TRACE:autologging.Class:__call__:RETURN 7
    7

    .. note::
       When the runtime Python version is >= 3.3, the *qualified* class
       name will be used to name the tracing logger (i.e. a nested class
       will write tracing log entries to a logger named
       "module.Parent.Nested").

    .. rubric:: Trace default methods using a named logger

    :arg logging.Logger logger:
       the parent logger used to trace the methods of the class

    By default, all "public", "_nonpublic", and "__internal" methods, as
    well as the special "__init__" method, will be traced. Tracing log
    entries will be written to the specified logger:

    >>> import sys
    >>> logging.basicConfig(
    ...     level=TRACE, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @traced(logging.getLogger("my.channel"))
    ... class Class:
    ...     def __init__(self, x):
    ...         self._x = x
    ...     def public(self, y):
    ...         return self._x + y
    ...     def _nonpublic(self, y):
    ...         return self._x - y
    ...     def __internal(self, y=2):
    ...         return self._x ** y
    ...     def __repr__(self):
    ...         return "Class(%r)" % self._x
    ...     def __call__(self):
    ...         return self._x
    ...
    >>> obj = Class(7)
    TRACE:my.channel.Class:__init__:CALL *(7,) **{}
    >>> obj.public(9)
    TRACE:my.channel.Class:public:CALL *(9,) **{}
    TRACE:my.channel.Class:public:RETURN 16
    16
    >>> obj._nonpublic(5)
    TRACE:my.channel.Class:_nonpublic:CALL *(5,) **{}
    TRACE:my.channel.Class:_nonpublic:RETURN 2
    2
    >>> obj._Class__internal(y=3)
    TRACE:my.channel.Class:__internal:CALL *() **{'y': 3}
    TRACE:my.channel.Class:__internal:RETURN 343
    343
    >>> repr(obj) # not traced by default
    'Class(7)'
    >>> obj()
    TRACE:my.channel.Class:__call__:CALL *() **{}
    TRACE:my.channel.Class:__call__:RETURN 7
    7

    .. rubric:: Trace specified methods using the default logger

    :arg tuple method_names:
       the names of the methods that will be traced

    Tracing log entries will be written to a logger named for the
    module and class:

    >>> import sys
    >>> logging.basicConfig(
    ...     level=TRACE, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @traced("public", "__internal")
    ... class Class:
    ...     def __init__(self, x):
    ...         self._x = x
    ...     def public(self, y):
    ...         return self._x + y
    ...     def _nonpublic(self, y):
    ...         return self._x - y
    ...     def __internal(self, y=2):
    ...         return self._x ** y
    ...     def __repr__(self):
    ...         return "Class(%r)" % self._x
    ...     def __call__(self):
    ...         return self._x
    ...
    >>> obj = Class(7)
    >>> obj.public(9)
    TRACE:autologging.Class:public:CALL *(9,) **{}
    TRACE:autologging.Class:public:RETURN 16
    16
    >>> obj._nonpublic(5)
    2
    >>> obj._Class__internal(y=3)
    TRACE:autologging.Class:__internal:CALL *() **{'y': 3}
    TRACE:autologging.Class:__internal:RETURN 343
    343
    >>> repr(obj)
    'Class(7)'
    >>> obj()
    7

    .. warning::
       When method names are specified explicitly via *args*,
       Autologging ensures that each method is actually defined in
       the body of the class being traced. (This means that inherited
       methods that are not overridden are **never** traced, even if
       they are named explicitly in *args*.)

       If a defintion for any named method is not found in the class
       body, either because the method is inherited or because the
       name is misspelled, Autologging will issue a :exc:`UserWarning`.

       If you wish to trace a method from a super class, you have two
       options:

       1. Use ``traced`` to decorate the super class.
       2. Override the method and trace it in the subclass.

    .. note::
       When the runtime Python version is >= 3.3, the *qualified* class
       name will be used to name the tracing logger (i.e. a nested class
       will write tracing log entries to a logger named
       "module.Parent.Nested").

    .. rubric:: Trace specified methods using a named logger

    :arg logging.Logger logger:
       the parent logger used to trace the methods of the class
    :arg tuple method_names:
       the names of the methods that will be traced

    >>> import sys
    >>> logging.basicConfig(
    ...     level=TRACE, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @traced(logging.getLogger("my.channel"), "public", "__internal")
    ... class Class:
    ...     def __init__(self, x):
    ...         self._x = x
    ...     def public(self, y):
    ...         return self._x + y
    ...     def _nonpublic(self, y):
    ...         return self._x - y
    ...     def __internal(self, y=2):
    ...         return self._x ** y
    ...     def __repr__(self):
    ...         return "Class(%r)" % self._x
    ...     def __call__(self):
    ...         return self._x
    ...
    >>> obj = Class(7)
    >>> obj.public(9)
    TRACE:my.channel.Class:public:CALL *(9,) **{}
    TRACE:my.channel.Class:public:RETURN 16
    16
    >>> obj._nonpublic(5)
    2
    >>> obj._Class__internal(y=3)
    TRACE:my.channel.Class:__internal:CALL *() **{'y': 3}
    TRACE:my.channel.Class:__internal:RETURN 343
    343
    >>> repr(obj) # not traced by default
    'Class(7)'
    >>> obj()
    7

    .. warning::
       When method names are specified explicitly via *args*,
       Autologging ensures that each method is actually defined in
       the body of the class being traced. (This means that inherited
       methods that are not overridden are **never** traced, even if
       they are named explicitly in *args*.)

       If a defintion for any named method is not found in the class
       body, either because the method is inherited or because the
       name is misspelled, Autologging will issue a :exc:`UserWarning`.

       If you wish to trace a method from a super class, you have two
       options:

       1. Use ``traced`` to decorate the super class.
       2. Override the method and trace it in the subclass.

    .. rubric:: Exclude specified methods from tracing

    .. versionadded:: 1.3.0

    :arg tuple method_names:
       the names of the methods that will be excluded from tracing
    :keyword bool exclude:
       ``True`` to cause the method names list to be interpreted as
       an exclusion list (``False`` is the default, and causes the named
       methods to be **included** as described above)

    The example below demonstrates exclusions using the default logger.

    >>> import sys
    >>> logging.basicConfig(
    ...     level=TRACE, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @traced("_nonpublic", "__internal", exclude=True)
    ... class Class:
    ...     def __init__(self, x):
    ...         self._x = x
    ...     def public(self, y):
    ...         return self._x + y
    ...     def _nonpublic(self, y):
    ...         return self._x - y
    ...     def __internal(self, y=2):
    ...         return self._x ** y
    ...     def __repr__(self):
    ...         return "Class(%r)" % self._x
    ...     def __call__(self):
    ...         return self._x
    ...
    >>> obj = Class(7)
    >>> obj.public(9)
    TRACE:autologging.Class:public:CALL *(9,) **{}
    TRACE:autologging.Class:public:RETURN 16
    16
    >>> obj._nonpublic(5)
    2
    >>> obj._Class__internal(y=3)
    343
    >>> repr(obj)
    'Class(7)'
    >>> obj()
    TRACE:autologging.Class:__call__:CALL *() **{}
    TRACE:autologging.Class:__call__:RETURN 7
    7

    When method names are excluded via *args* and the *exclude* keyword,
    Autologging **ignores** methods that are not actually defined in the
    body of the class being traced.

    .. warning::
       If an exclusion list causes the list of traceable methods to
       resolve empty, then Autologging will issue a :exc:`UserWarning`.

    .. note::
       When the runtime Python version is >= 3.3, the *qualified* class
       name will be used to name the tracing logger (i.e. a nested class
       will write tracing log entries to a logger named
       "module.Parent.Nested").

    .. note::
       When tracing a class, if the default (class-named) logger is
       used **and** the runtime Python version is >= 3.3, then the
       *qualified* class name will be used to name the tracing logger
       (i.e. a nested class will write tracing log entries to a logger
       named "module.Parent.Nested").

    .. note::
       If method names are specified when decorating a function, a
       :exc:`UserWarning` is issued, but the methods names are ignored
       and the function is traced as though the method names had not
       been specified.

    .. note::
       Both `Jython <http://www.jython.org/>`_ and
       `IronPython <http://ironpython.net/>`_ report an "internal" class
       name using its mangled form, which will be reflected in the
       default tracing logger name.

       For example, in the sample code below, both Jython and IronPython
       will use the default tracing logger name
       "autologging._Outer__Nested" (whereas CPython/PyPy/Stackless
       would use "autologging.__Nested" under Python 2 or
       "autologging.Outer.__Nested" under Python 3.3+)::

          class Outer:
             @traced
             class __Nested:
                pass

    .. warning::
       Neither `Jython <http://www.jython.org/>`_ nor
       `IronPython <http://ironpython.net/>`_ currently implement the
       ``function.__code__.co_lnotab`` attribute, so the last line
       number of a function cannot be determined by Autologging.

    .. versionchanged:: 1.3.1
       Due to unavoidable inconsistencies in line number tracking across
       Python variants (see
       `issues/6 <https://github.com/mzipay/Autologging/issues/6>`_, as
       of version 1.3.1 and until further notice Autologging will only
       record the first line number of the function being traced in all
       tracing CALL and RETURN records.
       (Note that YIELD tracing records for generator iterators will
       continue to record the correct line number on variants other than
       IronPython.)

    """
    return autologging.traced(*args, **kwargs)


def _add_logger_to(obj, logger_name=None, level=None, handlers=[], formatter="text", **kwargs):
    """Set a :class:`logging.Logger` member on *obj*.

    :arg obj: a class or function object
    :keyword str logger_name: the name (channel) of the logger for *obj*
    :keyword str formatter:
        logging formatter (text or json), if json JsonFormatter will be used 
        within all handlers. All other keyword arguments will be added to 
        json output rows as extra fields.
    :return: *obj*

    If *obj* is a class, the member will be named "__log". If *obj* is a
    function, the member will be named "_log".

    """
    if logger_name == "root":
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(logger_name if logger_name else autologging._generate_logger_name(obj))

    if level:
        logger.setLevel(level)

    for hdl in handlers:
        logger.addHandler(hdl)

    # Check logger has at lest 1 handler
    if len(logger.handlers) == 0:
        hdl = logging.StreamHandler()
        hdl.setLevel(logger.getEffectiveLevel())
        logger.addHandler(hdl)

    if formatter == "json":
        for handler in logger.handlers:
            handler.setFormatter(JsonFormatter(**kwargs))

    if isclass(obj):
        setattr(obj, autologging._mangle_name("__log", obj.__name__), logger)
    else:  # function
        obj._log = logger

    return obj


def logged(*args, **kwargs):
    """Add a logger member to a decorated class or function.

    :arg obj:
       the class or function object being decorated, or an optional
       :class:`logging.Logger` object to be used as the parent logger
       (instead of the default module-named logger)
    :keyword str level: logging level. 
    :keyword str formatter:
        logging formatter (text or json). If json, JsonFormatter will be used 
        within all handlers. All other keyword arguments will be added to 
        json output rows as extra fields.
    :return:
       *obj* if *obj* is a class or function; otherwise, if *obj* is a
       logger, return a lambda decorator that will in turn set the
       logger attribute and return *obj*

    If *obj* is a :obj:`class`, then ``obj.__log`` will have the logger
    name "<module-name>.<class-name>":

    >>> import sys
    >>> logging.basicConfig(
    ...     level=logging.DEBUG, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @logged
    ... class Sample:
    ...
    ...     def test(self):
    ...         self.__log.debug("This is a test.")
    ...
    >>> Sample().test()
    DEBUG:autologging.Sample:test:This is a test.

    .. note::
       Autologging will prefer to use the class's ``__qualname__`` when
       it is available (Python 3.3+). Otherwise, the class's
       ``__name__`` is used. For example::

          class Outer:

             @logged
             class Nested: pass

       Under Python 3.3+, ``Nested.__log`` will have the name
       "autologging.Outer.Nested", while under Python 2.7 or 3.2, the
       logger name will be "autologging.Nested".

    .. versionchanged:: 0.4.0
       Functions decorated with ``@logged`` use a *single* underscore
       in the logger variable name (e.g. ``my_function._log``) rather
       than a double underscore.

    If *obj* is a function, then ``obj._log`` will have the logger name
    "<module-name>":

    >>> import sys
    >>> logging.basicConfig(
    ...     level=logging.DEBUG, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @logged
    ... def test():
    ...     test._log.debug("This is a test.")
    ...
    >>> test()
    DEBUG:autologging:test:This is a test.

    .. note::
       Within a logged function, the ``_log`` attribute must be
       qualified by the function name.

    If *obj* is a :class:`logging.Logger` object, then that logger is
    used as the parent logger (instead of the default module-named
    logger):

    >>> import sys
    >>> logging.basicConfig(
    ...     level=logging.DEBUG, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @logged(logging.getLogger("test.parent"))
    ... class Sample:
    ...     def test(self):
    ...         self.__log.debug("This is a test.")
    ...
    >>> Sample().test()
    DEBUG:test.parent.Sample:test:This is a test.

    Again, functions are similar:

    >>> import sys
    >>> logging.basicConfig(
    ...     level=logging.DEBUG, stream=sys.stdout,
    ...     format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")
    >>> @logged(logging.getLogger("test.parent"))
    ... def test():
    ...     test._log.debug("This is a test.")
    ...
    >>> test()
    DEBUG:test.parent:test:This is a test.

    .. note::
       For classes, the logger member is made "private" (i.e. ``__log``
       with double underscore) to ensure that log messages that include
       the *%(name)s* format placeholder are written with the correct
       name.

       Consider a subclass of a ``@logged``-decorated parent class. If
       the subclass were **not** decorated with ``@logged`` and could
       access the parent's logger member directly to make logging
       calls, those log messages would display the name of the
       parent class, not the subclass.

       Therefore, subclasses of a ``@logged``-decorated parent class
       that wish to use a provided ``self.__log`` object must themselves
       be decorated with ``@logged``.

    .. warning::
       Although the ``@logged`` and ``@traced`` decorators will "do the
       right thing" regardless of the order in which they are applied to
       the same function, it is recommended that ``@logged`` always be
       used as the innermost decorator::

          @traced
          @logged
          def my_function():
              my_function._log.info("message")

       This is because ``@logged`` simply sets the ``_log`` attribute
       and then returns the original function, making it "safe" to use
       in combination with any other decorator.

    .. note::
       Both `Jython <http://www.jython.org/>`_ and
       `IronPython <http://ironpython.net/>`_ report an "internal" class
       name using its mangled form, which will be reflected in the
       default logger name.

       For example, in the sample code below, both Jython and IronPython
       will use the default logger name "autologging._Outer__Nested"
       (whereas CPython/PyPy/Stackless would use "autologging.__Nested"
       under Python 2 or "autologging.Outer.__Nested" under Python 3.3+)
       ::

          class Outer:
             @logged
             class __Nested:
                pass

    .. warning::
       `IronPython <http://ironpython.net/>`_ does not fully support
       frames (even with the -X:FullFrames option), so you are likely to
       see things like misreported line numbers and "<unknown file>" in
       log records emitted when running under IronPython.

    """
    obj = args[0] if args else None
    if obj is None:
        return lambda class_or_fn: _add_logger_to(class_or_fn, logger_name=None, **kwargs)
    if isinstance(obj, logging.Logger):  # `@logged(logger)'
        return lambda class_or_fn: _add_logger_to(class_or_fn, logger_name=obj.name, **kwargs)
    else:  # `@logged'
        return _add_logger_to(obj, **kwargs)


def lambdalogs(handler):
    """Lambda handler decorator that setup logging when handler is called.

    Adds ``request=context.request_id`` on all log messages.

    From environment variables:
    - ``BOTO_LOG_LEVEL`` set boto log level (default to ``WARN``);
    """

    @wraps(handler)
    def wrapper(self, event, **kwargs):
        try:
            request_id = event.payload["requestContext"]["requestId"]
        except (TypeError, KeyError):
            request_id = getattr(event.context, "aws_request_id", None)
        logger = getattr(self, autologging._mangle_name("__log", self.__class__.__name__))
        for hdl in logger.handlers:
            hdl.formatter.update_keywords(request_id=request_id)

        boto_level = os.getenv("BOTO_LOG_LEVEL", "WARNING")

        logging.getLogger("boto").setLevel(boto_level)
        logging.getLogger("boto3").setLevel(boto_level)
        logging.getLogger("botocore").setLevel(boto_level)

        return handler(self, event, **kwargs)

    return wrapper
