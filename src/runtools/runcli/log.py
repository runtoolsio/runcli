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

import logging
from functools import wraps
from logging import handlers

import sys
import time

from runtools.runcore import paths
from runtools.runcore.paths import expand_user

runtools_logger = logging.getLogger('runtools')
runtools_logger.propagate = False

log_timing = False

DEF_FORMATTER = logging.Formatter('%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')

STDOUT_HANDLER_NAME = 'stdout-handler'
STDERR_HANDLER_NAME = 'stderr-handler'
FILE_HANDLER_NAME = 'file-handler'


def configure(enabled, log_stdout_level='warn', log_file_level='info', log_file_path=None):
    runtools_logger.handlers.clear()
    runtools_logger.setLevel(logging.WARNING)

    if not enabled:
        runtools_logger.disabled = True
        return

    if log_stdout_level != 'off':
        level = logging.getLevelName(log_stdout_level.upper())
        setup_console(level)
        if level < runtools_logger.getEffectiveLevel():
            runtools_logger.setLevel(level)

    if log_file_level != 'off':
        level = logging.getLevelName(log_file_level.upper())
        log_file_path = expand_user(log_file_path) or paths.log_file_path(create=True)
        setup_file(level, log_file_path)
        if level < runtools_logger.getEffectiveLevel():
            runtools_logger.setLevel(level)


def is_disabled():
    return runtools_logger.disabled


def setup_console(level):
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.set_name(STDOUT_HANDLER_NAME)
    stdout_handler.setLevel(level)
    stdout_handler.setFormatter(DEF_FORMATTER)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    register_handler(stdout_handler)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.set_name(STDERR_HANDLER_NAME)
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(DEF_FORMATTER)
    stderr_handler.addFilter(lambda record: record.levelno > logging.INFO)
    register_handler(stderr_handler)


def get_console_level():
    return _get_handler_level(STDOUT_HANDLER_NAME)


def setup_file(level, file):
    file_handler = logging.handlers.WatchedFileHandler(file)
    file_handler.set_name(FILE_HANDLER_NAME)
    file_handler.setLevel(level)
    file_handler.setFormatter(DEF_FORMATTER)
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
                timer_logger.info(f'event=[timing] time=[{elapsed_time_ms:.2f} ms] op=[{operation}] args={log_args}')

            return result

        return wrapper

    return decorator
