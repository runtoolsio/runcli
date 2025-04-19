import argparse
import re
import textwrap

import sys
from rich_argparse import RichHelpFormatter

from runtools.runcore.run import TerminationStatus
from . import __version__

ACTION_JOB = 'job'
ACTION_SERVICE = 'service'
ACTION_CLEAN = 'clean'
ACTION_CONFIG = 'config'
ACTION_CONFIG_PRINT = 'print'
ACTION_CONFIG_CREATE = 'create'


def parse_args(args):
    parser = argparse.ArgumentParser(
        prog='run',
        description='Run managed job or service',
        formatter_class=RichHelpFormatter)
    parser.add_argument(
        "-V",
        "--version",
        action='version',
        help="Show version of this app and exit",
        version=__version__.__version__)

    parent = init_cfg_parent_parser()
    subparser = parser.add_subparsers(dest='action')  # command/action

    _init_config_parser(subparser)
    _init_job_parser(parent, subparser)
    _init_clean_parser(parent, subparser)

    parsed = parser.parse_args(args)
    if not getattr(parsed, 'action', None):
        parser.print_help()
        sys.exit(1)

    _check_conditions(parser, parsed)
    return parsed


def init_cfg_parent_parser():
    """
    Return:
        Parent parser for subparsers to share common configuration options
    """
    parser = argparse.ArgumentParser()
    cfg_group = parser.add_argument_group("Configuration options")
    cfg_group.description = """
        These options control how configuration is loaded. By default, runtools  searches for its configuration file 
        in standard XDG directories and loads settings from the first found file. If no config is found 
        then default one is used. These options allow you to modify this default behavior.
    """
    cfg_group.add_argument('-dc', '--def-config', action='store_true',
                           help='Do not lookup config file and use default configuration instead.')
    cfg_group.add_argument('-cr', '--config-required', action='store_true',
                           help='Configuration file must be found, otherwise the command will fail. (No fallback)')
    cfg_group.add_argument('-C', '--config', type=str,
                           help="Specifies path to config file stored in custom location. Fails if the file doesn't exist.")
    cfg_group.add_argument('--set', type=str, action='append',
                           help='Override value of config attribute. Format: attribute=value. Example: log.stdout.level=info')
    return parser


def _init_job_parser(parent, subparsers):
    """
    Creates parser for `exec` command

    :param parent: parent parser
    :param subparsers: sub-parser for exec parser to be added to
    """

    job_parser = subparsers.add_parser(
        ACTION_JOB,
        parents=[parent],
        description='Execute command',
        formatter_class=RichHelpFormatter,
        add_help=False)

    job_parser.description = textwrap.dedent("""
        Example of the execution: taro exec --id my_job ./my_job.sh arg1 arg2
        
        This is a main command of taro. It is used for managed execution of custom commands and applications.
        Taro provides number of features for commands executed this way. The main use case is a controlled execution of
        cron tasks. That is why command executed with taro is called a "job". Cronjob environment might not have
        taro binary executable on the path though. What usually works is to execute job explicitly using the python
        interpreter: `python3 -m taroapp exec CMD ARGS`. 
            
        It is recommended to use the `--id` option to specify the ID of the job otherwise the ID is constructed from the 
        command and its arguments. """)
    # General options
    job_parser.add_argument('--id', type=str,
                            help='Set the job ID. It is recommended to keep this value unset only for testing and '
                                 'development purposes.')
    job_parser.add_argument('--instance', type=str,
                            help='Set the instance ID. A unique value is generated when this option is not set. It '
                                 'is recommended to keep this value unique across all jobs.')
    job_parser.add_argument('-b', '--bypass-output', action='store_true',
                            help='Normally the output of the executed job is captured by taro where is processed '
                                 'and resend to standard streams. When this option is used taro does not capture '
                                 'the output from the job streams. This disables output based features, but it '
                                 'can help if there is any problem with output processing.')
    job_parser.add_argument('-o', '--no-overlap', action='store_true', default=False,
                            help='Skip if job with the same job ID is already running')
    job_parser.add_argument('-s', '--serial', action='store_true', default=False,
                            help='The execution will wait while there is a running job with the same job ID or a job '
                                 'belonging to the same execution group (if specified). As the name implies, '
                                 'this is used to achieve serial execution of the same (or same group of) jobs, '
                                 'i.e., to prevent parallel execution. The difference between this option and '
                                 '--no-overlap is that this option will not terminate the current job when a related '
                                 'job is executing, but puts this job in a waiting state instead. This option is a '
                                 'shortcut for the --max-executions 1 option (see help for more details).')
    job_parser.add_argument('-m', '--max-executions', type=int, default=0,
                            help='This option restricts the maximum number of parallel executions of the same job or '
                                 'jobs from the same execution group (if specified). If the current number of '
                                 'related executions prevents this job from being executed, then the job is put in a '
                                 'waiting state and resumed when the number of executions decreases. If there are '
                                 'more jobs waiting, the earlier ones have priority.')
    job_parser.add_argument('-q', '--queue-id', type=str,
                            help='Sets the queue ID for the job. The maximum number of simultaneous executions '
                                 'for all jobs belonging to the same execution queue can be specified using the '
                                 '`--serial` or `max-executions` options. If an execution queue is not set then '
                                 'it defaults to the job ID.')
    job_parser.add_argument('-A', '--approve', action='store_true', default=False,
                            help='Specifies pending group. The job will wait before execution in pending state'
                                 'until the group receives releasing signal. See the `release` command.')
    job_parser.add_argument('--warn-time', type=_warn_time_type, action='append', default=[],
                            help='This enables time warning which is trigger when the execution of the job exceeds '
                                 'the period specified by the value of this option. The value must be an integer '
                                 'followed by a single time unit character (one of [smhd]). For example `--warn-time '
                                 '1h` will trigger time warning when the job is executing over one hour.')
    job_parser.add_argument('--warn-output', type=str, action='append', default=[],
                            help='This enables output warning which is triggered each time an output line of the job '
                                 'matches regex specified by the value of this option. For example `--warn-output '
                                 '"ERR*"` triggers output warning each time an output line contains a word starting '
                                 'with ERR.')
    job_parser.add_argument('-d', '--depends-on', type=str, action='append', default=[],
                            help='The execution will be skipped if specified dependency job is not running.')
    job_parser.add_argument('-k', '--kv-filter', action='store_true', default=False,
                            help='Key-value output parser is used for task tracking.')
    job_parser.add_argument('--kv-alias', type=str, action='append', default=[],
                            help='Mapping of output keys to common fields.')
    job_parser.add_argument('-p', '--grok-pattern', type=str, action='append', default=[],
                            help='Grok pattern for extracting fields from output used for task tracking.')
    job_parser.add_argument('--dry-run', type=_str2_term_status, nargs='?', const=TerminationStatus.COMPLETED,
                            help='The job will be started without actual execution of its command. The final state '
                                 'of the job is specified by the value of this option. Default state is COMPLETED. '
                                 'This option can be used for testing some of the functionality like custom plugins.')
    job_parser.add_argument('-t', '--timeout', type=str,
                            help='The value of this option specifies the signal number or code for stopping the job '
                                 'due to a timeout. A timeout warning is added to the job when it is stopped in this '
                                 'way.')

    job_parser.add_argument('--param', type=lambda p: p.split('='), action='append',
                            help="Parameters are specified in `name=value` format. They represent metadata of the "
                                 "job instance and have no effect on the job execution. They are stored for the each "
                                 "execution and can be retrieved later. For example the `history` command has "
                                 "`--show-params` option to display `Parameters` column.")
    # Terms command and arguments taken from python doc and docker run help,
    # for this app (or rather exec command) these are operands (alternatively arguments)
    job_parser.add_argument('command', type=str, metavar='COMMAND', help='Program to execute')
    job_parser.add_argument('arg', type=str, metavar='ARG', nargs=argparse.REMAINDER, help="Program arguments")


def _init_clean_parser(common, subparsers):
    """
    Creates parsers for `clean` command

    :param common: parent parser
    :param subparsers: sub-parser for clean parser to be added to
    """

    clean_parser = subparsers.add_parser(ACTION_CLEAN, parents=[common], description='Performs cleanups',
                                         add_help=False)


def _init_config_parser(subparser):
    """
    Creates parsers for `config` command and its subcommands.
    :param subparser: sub-parser for config parser to be added to
    """
    config_parser = subparser.add_parser(
        ACTION_CONFIG,
        description='Manage config file',
        help='Manage config file',
        formatter_class=RichHelpFormatter)

    config_subparser = config_parser.add_subparsers(dest='config_action', required=True) # Actions under 'config'

    print_config_parser = config_subparser.add_parser(
        ACTION_CONFIG_PRINT,
        help='Print config file content',
        description='Print config file content. Default: prints loaded config from standard locations (e.g., XDG).',
        formatter_class=RichHelpFormatter)
    print_config_parser.add_argument(
        '-dc', '--def-config', action='store_true', help='Show default config file content.')

    create_config_parser = config_subparser.add_parser(
        ACTION_CONFIG_CREATE,
        help='Create new config file',
        description='Create new config file with defaults. Default location: standard user config dir (e.g., XDG_CONFIG_HOME).',
        formatter_class=RichHelpFormatter,
        add_help=True)
    create_config_parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite if config file exists.')
    create_config_parser.add_argument('-p', '--path', type=str, help='Specify path for created config file.')

# TODO Consider: change to str (like SortCriteria case) and remove this function
def _str2_term_status(v):
    try:
        return TerminationStatus[v.upper()]
    except KeyError:
        raise argparse.ArgumentTypeError('Arguments can be only valid execution states: '
                                         + ", ".join([e.name.lower() for e in TerminationStatus]))


def _warn_time_type(arg_value):
    regex = r'^\d+[smhd]$'
    pattern = re.compile(regex)
    if not pattern.match(arg_value):
        raise argparse.ArgumentTypeError(f"Execution time warning value {arg_value} does not match pattern {regex}")
    return arg_value


def _check_conditions(parser, parsed):
    _check_config_option_conflicts(parser, parsed)


def _check_config_option_conflicts(parser, parsed):
    """
    Check that incompatible combinations of options were not used

    :param parser: parser
    :param parsed: parsed arguments
    """
    config_options = []
    if hasattr(parsed, 'def_config') and parsed.def_config:
        config_options.append('def_config')
    if hasattr(parsed, 'config_required') and parsed.config_required:
        config_options.append('config_required')
    if hasattr(parsed, 'config') and parsed.config:
        config_options.append('config')

    if len(config_options) > 1:
        parser.error('Conflicting config options: ' + str(config_options))
