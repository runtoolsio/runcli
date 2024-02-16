"""
A command for running jobs.

TODO:
1. Output parsing
2. Coordination
3. Warnings
"""

import logging
import signal

from runtools import runner
from runtools.runcli.common import ProgramExecutionError
from runtools.runcore import util
from runtools.runcore.track import TaskTrackerMem
from runtools.runcore.util import KVParser, iso_date_time_parser
from runtools.runner import ExecutingPhase
from runtools.runner.program import ProgramExecution
from runtools.runner.task import Fields, OutputToTask
from runtools.runner.test.execution import TestExecution

log = logging.getLogger(__name__)


def run(args):
    job_id = args.id or " ".join([args.command] + args.arg)

    execution = resolve_execution(args)

    parsers = list(output_parsers(args))
    if parsers:
        task_tracker = TaskTrackerMem(job_id)
        output_to_task = OutputToTask(task_tracker, parsers)
        exec_phase = ExecutingPhase('Job Execution', execution, output_handlers=output_to_task.new_output)
    else:
        task_tracker = None
        exec_phase = ExecutingPhase('Job Execution', execution)

    job_instance = runner.job_instance(job_id, [exec_phase], task_tracker=task_tracker)
    _set_signal_handlers(job_instance, args.timeout)
    job_instance.run()

    if isinstance(execution, ProgramExecution) and execution.ret_code:
        if execution.ret_code > 0:
            raise ProgramExecutionError(execution.ret_code)
        if execution.ret_code < 0:
            raise ProgramExecutionError(abs(execution.ret_code) + 128)


def resolve_execution(args):
    if args.dry_run:
        return TestExecution(args.dry_run)
    return ProgramExecution(*([args.command] + args.arg), read_output=not args.bypass_output)


def output_parsers(args):
    if args.grok_pattern:
        from pygrok import Grok  # Defer import until is needed
        for grok_pattern in args.grok_pattern:
            yield Grok(grok_pattern).match
    if args.kv_filter:
        aliases = util.split_params(args.kv_alias)
        yield KVParser(aliases=aliases, post_parsers=[iso_date_time_parser(Fields.TIMESTAMP.value)])


def _set_signal_handlers(job_instance, timeout_signal):
    term = Term(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)

    if timeout_signal:
        if timeout_signal.isnumeric():
            timeout_signal_number = timeout_signal
        else:
            signal_enum = getattr(signal.Signals, timeout_signal)
            timeout_signal_number = signal_enum.value

        signal.signal(timeout_signal_number, term.timeout)


class Term:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def terminate(self, _, __):
        log.warning('event=[terminated_by_signal]')
        self.job_instance.stop()

    def timeout(self, _, __):
        log.warning('event=[terminated_by_timeout_signal]')
        # self.job_instance.add_warning(Warn('timeout'))  # TODO
        self.job_instance.stop()
