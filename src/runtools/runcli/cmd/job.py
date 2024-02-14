import logging
import signal

from runtools import runner
from runtools.runner import ExecutingPhase
from runtools.runner.program import ProgramExecution
from runtools.runner.test.execution import TestExecution

log = logging.getLogger(__name__)


def run(args):
    job_id = args.id or " ".join([args.command] + args.arg)

    if args.dry_run:
        execution = TestExecution(args.dry_run)
    else:
        execution = ProgramExecution(*([args.command] + args.arg), read_output=not args.bypass_output)

    exec_phase = ExecutingPhase('Job Execution', execution)

    job_instance = runner.job_instance(job_id, [exec_phase])
    _set_signal_handlers(job_instance, args.timeout)
    job_instance.run()


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
