from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase, ApprovalPhase, ExecutionQueue, ExecutionGroup
from runtools.runjob.program import ProgramPhase


def run(instance_id, env_config, program_args, *,
        bypass_output=False, excl=False, approve_id=None, serial=False):
    phases = create_phases(instance_id, program_args, bypass_output, excl, approve_id, serial)
    with node.create(env_config) as env_node:
        env_node.create_instance(instance_id, phases).run()


def create_phases(instance_id, program_args, bypass_output, excl, approve_id, serial):
    if excl and serial:
        raise ValueError("Exclusive run cannot be used with serial")

    phases = []

    if approve_id:
        phases.append(ApprovalPhase(phase_id=approve_id, phase_name='Run Manual Approval'))

    program_phase = ProgramPhase('PROGRAM', *program_args, read_output=not bypass_output)
    if excl:
        phases.append(MutualExclusionPhase('MUTEX_GUARD', program_phase, exclusion_group=instance_id.job_id))
    elif serial:
        phases.append(ExecutionQueue('QUEUE', ExecutionGroup(instance_id.job_id, 1), program_phase))
    else:
        phases.append(program_phase)
    return phases
