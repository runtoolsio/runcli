from runtools.runcore.job import InstanceID
from runtools.runjob import environment
from runtools.runjob.program import ProgramPhase


def run(args):
    job_id = args.id or " ".join([args.command.removeprefix('./')] + args.arg)
    instance_id = InstanceID(job_id)
    phase = ProgramPhase('PROGRAM', *([args.command] + args.arg))
    with environment.local() as env:
        env.create_instance(instance_id, [phase]).run()
