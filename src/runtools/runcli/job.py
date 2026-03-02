import logging
import signal
from re import PatternError

from runtools.runcore.db import DuplicateInstanceError
from runtools.runcore.job import InstanceID
from runtools.runcore.run import StopReason
from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase, CheckpointPhase, ExecutionQueue, ConcurrencyGroup
from runtools.runjob.phase import TimeoutExtension, SequentialPhase
from runtools.runjob.program import ProgramPhase
from runtools.runjob.warning import TimeWarningExtension, OutputWarningExtension

logger = logging.getLogger(__name__)


def run(instance_id, env_config, program_args, *,
        bypass_output=False,
        no_output_storage=False,
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
        output_sink=None,
        tail_buffer_size=None,
        max_reruns=0,
        ):
    root_phase = create_root_phase(instance_id, program_args, bypass_output, excl, excl_group, checkpoint_id, serial,
                                   max_concurrent, concurrency_group, timeout, time_warning, output_warning)

    if no_output_storage:
        env_config.output.storages = []
    if tail_buffer_size is not None:
        env_config.output.tail_buffer_size = tail_buffer_size

    with node.create(env_config) as env_node:
        inst = _create_with_rerun_suffix(env_node, instance_id, root_phase, output_sink, max_reruns)
        _set_signal_handlers(inst, timeout_signal)
        inst.run()


def _create_with_rerun_suffix(env_node, instance_id, root_phase, output_sink, max_reruns):
    """Create instance, suffixing the run ID (-2, -3, ...) on duplicate up to max_reruns times."""
    try:
        return env_node.create_instance(instance_id, root_phase, output_sink=output_sink)
    except DuplicateInstanceError:
        if max_reruns <= 0:
            raise

    for suffix in range(2, max_reruns + 2):
        rerun_id = InstanceID(instance_id.job_id, f"{instance_id.run_id}-{suffix}")
        try:
            inst = env_node.create_instance(rerun_id, root_phase, output_sink=output_sink)
            logger.info("event=[rerun_suffix] run_id=[%s]", rerun_id.run_id)
            return inst
        except DuplicateInstanceError:
            continue

    raise DuplicateInstanceError(instance_id)


def create_root_phase(instance_id, program_args, bypass_output, excl, excl_group, checkpoint_id, serial, max_concurrent,
                      concurrency_group, timeout, time_warning, output_warning):
    if serial and max_concurrent:
        raise ValueError("Either `serial` or `max_concurrent` can be set")

    phase = ProgramPhase('EXEC', *program_args, read_output=not bypass_output)
    if excl or excl_group:
        phase = MutualExclusionPhase('MUTEX_GUARD', phase, exclusion_group=excl_group)
    if serial or max_concurrent:
        phase = ExecutionQueue(
            'QUEUE', ConcurrencyGroup(concurrency_group or instance_id.job_id, max_concurrent or 1), phase)

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
            logger.warning(f"invalid_output_warning_pattern detail=[{e}] result=[Output warning disabled]")

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
        if timeout_signal.isnumeric():
            timeout_signal_number = timeout_signal
        else:
            signal_enum = getattr(signal.Signals, timeout_signal)
            timeout_signal_number = signal_enum.value

        signal.signal(timeout_signal_number, term.timeout)
