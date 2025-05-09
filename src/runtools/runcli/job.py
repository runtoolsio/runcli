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
        concurrency_group=None):
    phases = create_phases(instance_id, program_args, bypass_output, excl, excl_group, approve_id, serial,
                           max_concurrent, concurrency_group)
    with node.create(env_config) as env_node:
        env_node.create_instance(instance_id, phases).run()


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
