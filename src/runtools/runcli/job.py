from runtools.runcore import env
from runtools.runcore.env import DEFAULT_ENVIRONMENT, EnvironmentConfigUnion
from runtools.runcore.job import InstanceID
from runtools.runjob import node
from runtools.runjob.program import ProgramPhase


def run(def_env, args):
    env_config = env.get_env_config(getattr(args, 'env') or def_env, fallback_default=(def_env == DEFAULT_ENVIRONMENT))
    job_id = args.id or " ".join([args.command.removeprefix('./')] + args.arg)
    instance_id = InstanceID(job_id)
    phase = ProgramPhase('PROGRAM', *([args.command] + args.arg))
    with node.create(env_config) as env_node:
        env_node.create_instance(instance_id, [phase]).run()
