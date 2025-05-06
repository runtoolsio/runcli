from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase, ApprovalPhase
from runtools.runjob.program import ProgramPhase


def run(instance_id, env_config, program_args, *, excl=False, approve_id=None):
    phases = create_phases(instance_id, program_args, excl, approve_id)
    with node.create(env_config) as env_node:
        env_node.create_instance(instance_id, phases).run()


def create_phases(instance_id, program_args, excl, approve_id):
    phases = []

    if approve_id:
        phases.append(ApprovalPhase(phase_id=approve_id, phase_name='Run Manual Approval'))

    program_phase = ProgramPhase('PROGRAM', *program_args)
    if excl:
        phases.append(MutualExclusionPhase(instance_id.job_id, program_phase))
    else:
        phases.append(program_phase)
    return phases
