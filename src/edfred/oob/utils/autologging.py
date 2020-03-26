"""Microlibrary to simplify logging in AWS Lambda."""
import logging
import json
import os
from inspect import isclass
from functools import wraps
import autologging


__all__ = ["TRACE", "logged", "traced", "lambdalogs"]


TRACE = autologging.TRACE


def _json_formatter(obj):
    """Formatter for unserialisable values."""
    return str(obj)


class JsonFormatter(logging.Formatter):
    """AWS Lambda Logging formatter.

    Formats the log message as a JSON encoded string.  If the message is a
    dict it will be used directly.  If the message can be parsed as JSON, then
    the parse d value is used in the output record.
    """

    def __init__(self, **kwargs):
        """Return a JsonFormatter instance.

        The `json_default` kwarg is used to specify a formatter for otherwise
        unserialisable values.  It must not throw.  Defaults to a function that
        coerces the value to a string.

        Other kwargs are used to specify log field format strings.
        """
        datefmt = kwargs.pop("datefmt", None)

        super(JsonFormatter, self).__init__(datefmt=datefmt)
        self.format_dict = {
            "timestamp": "%(asctime)s",
            "level": "%(levelname)s",
            "filename": "%(filename)s",
            "location": "%(name)s.%(funcName)s:%(lineno)d",
        }
        self.update_keywords(**kwargs)

    def update_keywords(self, **kwargs):
        self.format_dict.update(kwargs)
        self.default_json_formatter = kwargs.pop("json_default", _json_formatter)

    def format(self, record):
        record_dict = record.__dict__.copy()
        record_dict["asctime"] = self.formatTime(record, self.datefmt)

        log_dict = {k: v % record_dict for k, v in self.format_dict.items() if v}

        if isinstance(record_dict["msg"], dict):
            log_dict["message"] = record_dict["msg"]
        else:
            log_dict["message"] = record.getMessage()

            # Attempt to decode the message as JSON, if so, merge it with the
            # overall message for clarity.
            try:
                log_dict["message"] = json.loads(log_dict["message"])
            except (TypeError, ValueError):
                pass

        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            # from logging.Formatter:format
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            log_dict["exception"] = record.exc_text

        json_record = json.dumps(log_dict, default=self.default_json_formatter)

        if hasattr(json_record, "decode"):  # pragma: no cover
            json_record = json_record.decode("utf-8")

        return json_record


def traced(*args, **kwargs):
    """Add call and return tracing to an unbound function or to the
	methods of a class.

	The arguments to ``traced`` differ depending on whether it is being
	used to trace an unbound function or the methods of a class:

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
