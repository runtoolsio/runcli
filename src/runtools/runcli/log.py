"""
This module is used for configuring the `runtools` logger. The convention is to use this logger as a parent logger
in all runtools libraries, plugins, and related components. In doing so, all logging related to runtools is unified
and can be configured from a single location (this module).

By default, no handlers are provided and propagation is turned off. If any logging is needed, the user of the library
can use this module to set up logging according to their needs. This can be done either by adding their own handlers
using the `register_handler()` function, or by using one of the pre-configured modes represented by the cfg.LogMode
enum passed to the `configure()` function. See the `cfg.LogMode` documentation for details.

When `LogMode.ENABLED` is set, stdout+stderr and/or file custom handlers are be registered.

When `LogMode.DISABLED` is set, events of severity WARNING and higher will still be printed to stderr.
A `logging.NullHandler` can be added to disable this behavior.

Note:
    This module follows the recommendations specified in the official documentation:
    https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library.
"""

import json
import logging
from datetime import datetime, timezone
from functools import wraps
from logging import handlers

import sys
import time

from runtools.runcore import paths
from runtools.runcore.err import RuntoolsException
from runtools.runcore.paths import expand_user
from runtools.runjob.log import RunContextFilter

LOG_FILENAME = 'runcli.log'

runtools_logger = logging.getLogger('runtools')
runtools_logger.propagate = False

log_timing = False

_LOG_RECORD_BUILTINS = frozenset({
    'name', 'msg', 'args', 'created', 'filename', 'funcName', 'levelname',
    'levelno', 'lineno', 'module', 'msecs', 'pathname', 'process',
    'processName', 'relativeCreated', 'stack_info', 'exc_info', 'exc_text',
    'thread', 'threadName', 'taskName', 'message',
})


def _collect_extras(record) -> dict:
    """Collect non-builtin fields from a LogRecord (extras + filter-injected context)."""
    return {k: v for k, v in record.__dict__.items()
            if k not in _LOG_RECORD_BUILTINS and not k.startswith('_') and v is not None}


class PlainFormatter(logging.Formatter):
    """Plain text formatter that appends extra fields as key=value."""

    def format(self, record):
        msg = f"{record.levelname} - {record.getMessage()}"
        extras = _collect_extras(record)
        if extras:
            msg += ' ' + ' '.join(f"{k}={v}" for k, v in extras.items())
        if record.exc_info and record.exc_info[1]:
            msg += '\n' + self.formatException(record.exc_info)
        return msg


class JsonFormatter(logging.Formatter):
    """JSON Lines formatter for structured file logging.

    Produces one JSON object per line with keys matching the OutputLine JSONL convention:
    ``ts``, ``lvl``, ``logger``, ``msg``, plus any extra fields and run context.
    """

    def format(self, record):
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec='milliseconds')
        data = {
            "ts": ts[:-6] + 'Z' if ts.endswith('+00:00') else ts,
            "lvl": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        data.update(_collect_extras(record))
        if record.exc_info and record.exc_info[1]:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str, ensure_ascii=False)

DEF_LEVEL_STDOUT = 'WARN'
DEF_LEVEL_FILE = 'DEBUG'

STDOUT_HANDLER_NAME = 'stdout-handler'
STDERR_HANDLER_NAME = 'stderr-handler'
FILE_HANDLER_NAME = 'file-handler'


_run_context_filter = RunContextFilter()


def configure(enabled, log_stdout_level=DEF_LEVEL_STDOUT, log_file_level=DEF_LEVEL_FILE, log_file_path=None):
    runtools_logger.handlers.clear()
    runtools_logger.setLevel(logging.WARNING)

    if not enabled:
        runtools_logger.disabled = True
        return

    if log_stdout_level != 'off':
        level_error = False
        level = logging.getLevelName(log_stdout_level.upper())
        if not isinstance(level, int):
            level = logging.getLevelName(DEF_LEVEL_STDOUT)
            level_error = True

        setup_console(level)

        if level < runtools_logger.getEffectiveLevel():
            runtools_logger.setLevel(level)
        if level_error:
            runtools_logger.warning("Invalid log level", extra={"type": "stdout", "level": log_stdout_level, "default": DEF_LEVEL_STDOUT})

    if log_file_level != 'off':
        level_error = False
        level = logging.getLevelName(log_file_level.upper())
        if not isinstance(level, int):
            level = logging.getLevelName(DEF_LEVEL_FILE)
            level_error = True
        try:
            log_file_path = expand_user(log_file_path) or (paths.log_dir(create=True) / LOG_FILENAME)
            setup_file(level, log_file_path)
        except OSError as e:
            print(f"WARNING: File logging disabled: {e}", file=sys.stderr)
        else:
            if level < runtools_logger.getEffectiveLevel():
                runtools_logger.setLevel(level)
            if level_error:
                runtools_logger.warning("Invalid log level", extra={"type": "file", "level": log_file_level, "default": DEF_LEVEL_FILE})


def is_disabled():
    return runtools_logger.disabled


def setup_console(level):
    formatter = PlainFormatter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.set_name(STDOUT_HANDLER_NAME)
    stdout_handler.setLevel(level)
    stdout_handler.setFormatter(formatter)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    stdout_handler.addFilter(_run_context_filter)
    register_handler(stdout_handler)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.set_name(STDERR_HANDLER_NAME)
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(formatter)
    stderr_handler.addFilter(lambda record: record.levelno > logging.INFO)
    stderr_handler.addFilter(_run_context_filter)
    register_handler(stderr_handler)


def get_console_level():
    return _get_handler_level(STDOUT_HANDLER_NAME)


def setup_file(level, file):
    file_handler = logging.handlers.WatchedFileHandler(file)
    file_handler.set_name(FILE_HANDLER_NAME)
    try:
        file_handler.setLevel(level)
    except ValueError as e:
        raise InvalidLogLevelError(str(e))
    file_handler.setFormatter(JsonFormatter())
    file_handler.addFilter(_run_context_filter)
    register_handler(file_handler)


def get_file_level():
    return _get_handler_level(FILE_HANDLER_NAME)


def get_file_path():
    handler = _find_handler(FILE_HANDLER_NAME)
    if handler:
        return handler.baseFilename
    else:
        return None


def _find_handler(name):
    for handler in runtools_logger.handlers:
        if handler.name == name:
            return handler

    return None


def register_handler(handler):
    previous = _find_handler(handler.name)
    if previous:
        runtools_logger.removeHandler(previous)

    runtools_logger.addHandler(handler)


def _get_handler_level(name):
    handler = _find_handler(name)
    return handler.level if handler else None


def timing(operation, *, args_idx=()):
    timer_logger = logging.getLogger('runtools.timer')

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            if log_timing:
                log_args = []
                for i in args_idx:
                    if i >= len(args):
                        break
                    log_args.append(args[i])
                elapsed_time_ms = (time.time() - start_time) * 1000
                timer_logger.info("Timing", extra={"time_ms": round(elapsed_time_ms, 2), "op": operation, "args": str(log_args)})

            return result

        return wrapper

    return decorator


class InvalidLogLevelError(RuntoolsException):
    pass
