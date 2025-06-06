import logging
import signal
from re import PatternError

from runtools.runcore.run import StopReason
from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase, ApprovalPhase, ExecutionQueue, ConcurrencyGroup
from runtools.runjob.phase import SequentialPhase, TimeoutExtension
from runtools.runjob.program import ProgramPhase
from runtools.runjob.warning import TimeWarningExtension, OutputWarningExtension

logger = logging.getLogger(__name__)


def run(instance_id, env_config, program_args, *,
        bypass_output=False,
        excl=False,
        excl_group=None,
        approve_id=None,
        serial=False,
        max_concurrent=0,
        concurrency_group=None,
        timeout=0.0,
        timeout_signal=None,
        time_warning=None,
        output_warning=(),
        ):
    root_phase = create_root_phase(instance_id, program_args, bypass_output, excl, excl_group, approve_id, serial,
                                   max_concurrent, concurrency_group, timeout, time_warning, output_warning)
    with node.create(env_config) as env_node:
        inst = env_node.create_instance(instance_id, root_phase)
        _set_signal_handlers(inst, timeout_signal)
        inst.run()


def create_root_phase(instance_id, program_args, bypass_output, excl, excl_group, approve_id, serial, max_concurrent,
                      concurrency_group, timeout, time_warning, output_warning):
    if serial and max_concurrent:
        raise ValueError("Either `serial` or `max_concurrent` can be set")

    phase = ProgramPhase('EXEC', *program_args, read_output=not bypass_output)
    if excl or excl_group:
        phase = MutualExclusionPhase('MUTEX_GUARD', phase, exclusion_group=excl_group)
    if serial or max_concurrent:
        phase = ExecutionQueue(
            'QUEUE', ConcurrencyGroup(concurrency_group or instance_id.job_id, max_concurrent or 1), phase)

    if approve_id:
        phase = ApprovalPhase(phase_id=approve_id, phase_name='Run Manual Approval', children=(phase,))

    root_phase = SequentialPhase('root', [phase])
    if timeout:
        root_phase = TimeoutExtension(root_phase, timeout)
    if time_warning:
        root_phase = TimeWarningExtension(root_phase, time_warning)
    if output_warning:
        try:
            root_phase = OutputWarningExtension(root_phase, output_warning)
        except PatternError as e:
            logger.warning(f"invalid_output_warning_pattern detail=[{e}] result=[Output warning disabled]")
    return root_phase


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
        if timeout_signal.isnumeric():
            timeout_signal_number = timeout_signal
        else:
            signal_enum = getattr(signal.Signals, timeout_signal)
            timeout_signal_number = signal_enum.value

        signal.signal(timeout_signal_number, term.timeout)
