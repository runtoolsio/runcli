import signal

from runtools.runcore.run import StopReason
from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase, ApprovalPhase, ExecutionQueue, ConcurrencyGroup
from runtools.runjob.program import ProgramPhase


def run(instance_id, env_config, program_args, *,
        bypass_output=False,
        excl=False,
        excl_group=None,
        approve_id=None,
        serial=False,
        max_concurrent=0,
        concurrency_group=None,
        timeout_signal=None):
    phases = create_phases(instance_id, program_args, bypass_output, excl, excl_group, approve_id, serial,
                           max_concurrent, concurrency_group)
    with node.create(env_config) as env_node:
        inst = env_node.create_instance(instance_id, phases)
        _set_signal_handlers(inst, timeout_signal)
        inst.run()


def create_phases(instance_id, program_args, bypass_output, excl, excl_group, approve_id, serial, max_concurrent,
                  concurrency_group):
    if serial and max_concurrent:
        raise ValueError("Either `serial` or `max_concurrent` can be set")

    phases = []

    if approve_id:
        phases.append(ApprovalPhase(phase_id=approve_id, phase_name='Run Manual Approval'))

    program_phase = ProgramPhase('PROGRAM', *program_args, read_output=not bypass_output)
    if excl or excl_group:
        exec_phase = MutualExclusionPhase('MUTEX_GUARD', program_phase, exclusion_group=excl_group)
    else:
        exec_phase = program_phase
    if serial or max_concurrent:
        phases.append(
            ExecutionQueue('QUEUE', ConcurrencyGroup(concurrency_group or instance_id.job_id, max_concurrent or 1),
                           exec_phase))
    else:
        phases.append(exec_phase)
    return phases


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
