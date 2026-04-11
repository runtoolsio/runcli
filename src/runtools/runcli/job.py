import logging
import signal
from re import error as PatternError

from runtools.runcore.job import DuplicateStrategy
from runtools.runcore.run import StopReason
from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase, CheckpointPhase, ExecutionQueue, ConcurrencyGroup
from runtools.runjob.phase import TimeoutExtension, SequentialPhase
from runtools.runjob.program import ProgramPhase
from runtools.runjob.warning import TimeWarningExtension, OutputWarningExtension

logger = logging.getLogger(__name__)


def run(job_id, run_id, env_id, program_args, *,
        bypass_output=False,
        disable_output=(),
        excl=False,
        excl_group=None,
        checkpoint_id=None,
        serial=False,
        max_concurrent=0,
        concurrency_group=None,
        timeout=0.0,
        timeout_signal=None,
        time_warning=None,
        output_warning=(),
        output_processors=(),
        tail_buffer_size=None,
        duplicate_strategy=DuplicateStrategy.DISALLOW,
        ):
    root_phase = create_root_phase(job_id, program_args, bypass_output, excl, excl_group, checkpoint_id, serial,
                                   max_concurrent, concurrency_group, timeout, time_warning, output_warning)

    with node.connect(env_id, disable_output=disable_output, tail_buffer_size=tail_buffer_size) as env_node:
        inst = env_node.create_instance(
            job_id, run_id, root_phase, output_processors=output_processors, duplicate_strategy=duplicate_strategy)
        _set_signal_handlers(inst, timeout_signal)
        inst.run()


def create_root_phase(job_id, program_args, bypass_output, excl, excl_group, checkpoint_id, serial, max_concurrent,
                      concurrency_group, timeout, time_warning, output_warning):
    """Build the root phase tree from CLI arguments."""
    if serial and max_concurrent:
        raise ValueError("Either `serial` or `max_concurrent` can be set")

    phase = ProgramPhase('EXEC', *program_args, read_output=not bypass_output)
    if excl or excl_group:
        phase = MutualExclusionPhase('MUTEX_GUARD', phase, exclusion_group=excl_group)
    if serial or max_concurrent:
        phase = ExecutionQueue(
            'QUEUE', ConcurrencyGroup(concurrency_group or job_id, max_concurrent or 1), phase)

    if checkpoint_id:
        checkpoint = CheckpointPhase(checkpoint_id)
        phase = SequentialPhase(f'{checkpoint_id}_seq', [checkpoint, phase])

    if timeout:
        phase = TimeoutExtension(phase, timeout)
    if time_warning:
        phase = TimeWarningExtension(phase, time_warning)
    if output_warning:
        try:
            phase = OutputWarningExtension(phase, output_warning)
        except PatternError as e:
            logger.warning("Invalid output warning pattern", extra={"detail": str(e)})

    return phase


class Sig:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def terminate(self, _, __):
        self.job_instance.stop(StopReason.SIGNAL)

    def timeout(self, _, __):
        # self.job_instance.task_tracker.warning('timeout')  TODO
        self.job_instance.stop(StopReason.TIMEOUT)


def _set_signal_handlers(job_instance, timeout_signal):
    term = Sig(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)

    if timeout_signal:
        try:
            if timeout_signal.isnumeric():
                timeout_signal_number = signal.Signals(int(timeout_signal)).value
            else:
                timeout_signal_number = signal.Signals[timeout_signal].value
        except (KeyError, ValueError):
            raise SystemExit(f"error: invalid signal: {timeout_signal}")

        signal.signal(timeout_signal_number, term.timeout)
