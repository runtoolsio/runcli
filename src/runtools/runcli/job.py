import logging
import os
import signal
from pathlib import Path
from re import PatternError

from runtools.runcore import paths
from runtools.runcore.env import FileOutputStorageConfig
from runtools.runcore.run import StopReason
from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase, CheckpointPhase, ExecutionQueue, ConcurrencyGroup
from runtools.runjob.output import FileOutputStorage, OutputRouter, InMemoryTailBuffer
from runtools.runjob.phase import TimeoutExtension, SequentialPhase
from runtools.runjob.program import ProgramPhase
from runtools.runjob.warning import TimeWarningExtension, OutputWarningExtension

logger = logging.getLogger(__name__)


def run(instance_id, env_config, program_args, *,
        bypass_output=False,
        no_output_file=False,
        output_path=None,
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
        tail_buffer_size=2 * 1024 * 1024,
        ):
    root_phase = create_root_phase(instance_id, program_args, bypass_output, excl, excl_group, checkpoint_id, serial,
                                   max_concurrent, concurrency_group, timeout, time_warning, output_warning)
    output_router = create_output_router(
        env_config.id, instance_id,
        no_output_file=no_output_file, output_path=output_path,
        output_storage_configs=getattr(env_config, 'output_storage', ()),
        tail_buffer_size=tail_buffer_size,
    )
    with node.create(env_config) as env_node:
        inst = env_node.create_instance(instance_id, root_phase,
                                        output_sink=output_sink, output_router=output_router)
        _set_signal_handlers(inst, timeout_signal)
        inst.run()


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
        checkpoint = CheckpointPhase(checkpoint_id, phase_name='Manual Checkpoint')
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


def create_output_router(env_id: str, instance_id, *,
                         no_output_file=False, output_path=None,
                         output_storage_configs=(),
                         tail_buffer_size: int = 2 * 1024 * 1024):
    storages = []

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        storages.append(FileOutputStorage(output_path))
    elif not no_output_file:
        for cfg in output_storage_configs:
            if not cfg.enabled:
                continue
            if isinstance(cfg, FileOutputStorageConfig):
                base = Path(cfg.dir).expanduser() if cfg.dir else paths.output_dir(env_id, create=True)
                path = base / instance_id.job_id / f"{instance_id.run_id}.jsonl"
                os.makedirs(path.parent, exist_ok=True)
                storages.append(FileOutputStorage(path))

    return OutputRouter(tail_buffer=InMemoryTailBuffer(max_bytes=tail_buffer_size), storages=storages)


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
