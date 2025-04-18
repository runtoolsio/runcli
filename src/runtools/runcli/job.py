from runtools.runjob import environment
from runtools.runjob.program import ProgramPhase


def run(args):
    job_id = args.id or " ".join([args.command.removeprefix('./')] + args.arg)
    phase = ProgramPhase('PROGRAM', *([args.command] + args.arg))
    with environment.local() as env:
        env.create_instance(job_id, [phase]).run()
