from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase
from runtools.runjob.program import ProgramPhase


def run(instance_id, env_config, args):
    # TODO Log term status
    with node.create(env_config) as env_node:
        env_node.create_instance(instance_id, create_phases(instance_id.job_id, args)).run()


def create_phases(job_id, args):
    phases = []
    program_phase = ProgramPhase('PROGRAM', *([args.command] + args.arg))
    if args.exclusive_run:
        phases.append(MutualExclusionPhase(job_id, program_phase))
    else:
        phases.append(program_phase)
    return phases
