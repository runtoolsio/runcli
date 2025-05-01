from runtools.runcore import env
from runtools.runcore.env import DEFAULT_ENVIRONMENT
from runtools.runcore.job import InstanceID
from runtools.runjob import node
from runtools.runjob.coord import MutualExclusionPhase
from runtools.runjob.program import ProgramPhase


def run(def_env, args):
    env_config = env.get_env_config(getattr(args, 'env') or def_env, fallback_default=(def_env == DEFAULT_ENVIRONMENT))
    job_id = args.id or " ".join([args.command.removeprefix('./')] + args.arg)
    instance_id = InstanceID(job_id, getattr(args, 'run_id'))
    with node.create(env_config) as env_node:
        env_node.create_instance(instance_id, create_phases(job_id, args)).run()


def create_phases(job_id, args):
    phases = []
    program_phase = ProgramPhase('PROGRAM', *([args.command] + args.arg))
    if args.exclusive_run:
        phases.append(MutualExclusionPhase(job_id, program_phase))
    else:
        phases.append(program_phase)
    return phases
