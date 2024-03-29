import argparse
import re
import textwrap
from argparse import RawTextHelpFormatter

import sys

from runtools.runcore.run import TerminationStatus

ACTION_JOB = 'job'
ACTION_SERVICE = 'service'
ACTION_CLEAN = 'clean'
ACTION_SETUP = 'setup'
ACTION_SETUP_CONFIG = 'config'
ACTION_CONFIG_PRINT = 'print'
ACTION_CONFIG_CREATE = 'create'
ACTION_HOSTINFO = 'hostinfo'


def parse_args(args):
    # TODO destination required
    parser = argparse.ArgumentParser(prog='run', description='Run managed job or service')
    parser.add_argument("-V", "--version", action='version', help="Show version of runcli and exit", version='0.1.0')  # TODO Version
    common = argparse.ArgumentParser()  # parent parser for subparsers in case they need to share common options
    init_cfg_group(common)
    subparser = parser.add_subparsers(dest='action')  # command/action

    _init_job_parser(common, subparser)
    _init_clean_parser(common, subparser)
    _init_setup_parser(subparser)
    _init_hostinfo_parser(common, subparser)

    parsed = parser.parse_args(args)
    if not getattr(parsed, 'action', None):
        parser.print_help()
        sys.exit(1)

    _check_conditions(parser, parsed)
    return parsed


def init_cfg_group(common):
    cfg_group = common.add_argument_group("Configuration options")
    cfg_group.description = """
        These options affects the way how the configuration is loaded and set.
        By default the configuration file located in one of the XDG directories is loaded and its content
        overrides values of the cfg module. Changing this default behaviour is not needed under normal usage.
        Therefore these options are usually used only during testing, experimenting and debugging.
        More details in the config doc: https://github.com/taro-suite/taro/blob/master/CONFIG.md
    """
    cfg_group.add_argument('-dc', '--def-config', action='store_true',
                           help='Use configuration stored in default config file. Run `taro config show -dc` to see '
                                'the content of the file.')
    cfg_group.add_argument('-mc', '--min-config', action='store_true',
                           help='Do not load any config file and use minimal configuration instead. Check CONFIG.md '
                                'for minimal configuration values.')
    cfg_group.add_argument('-C', '--config', type=str,
                           help='Load a config file stored in a custom location. The value of this option is the path '
                                'to the custom config file.')
    cfg_group.add_argument('--set', type=str, action='append',
                           help='Override value of a configuration attribute. The value format is: attribute=value. '
                                'See CONFIG.md for attributes details. This option can be used multiple times.')


def _init_job_parser(common, subparsers):
    """
    Creates parser for `exec` command

    :param common: parent parser
    :param subparsers: sub-parser for exec parser to be added to
    """

    job_parser = subparsers.add_parser(
        ACTION_JOB, formatter_class=RawTextHelpFormatter, parents=[common], description='Execute command',
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


def _init_setup_parser(subparser):
    """
    Creates parsers for `setup` command

    :param subparser: sub-parser for setup parser to be added to
    """

    setup_parser = subparser.add_parser(ACTION_SETUP, description='Setup related actions')
    setup_parser.add_argument('--no-color', action='store_true', help='do not print colours in output')

    setup_subparser = setup_parser.add_subparsers(dest='setup_action')
    config_parser = setup_subparser.add_parser(ACTION_SETUP_CONFIG, help='Config related commands')
    config_subparser = config_parser.add_subparsers(dest='config_action')

    show_config_parser = config_subparser.add_parser(
        ACTION_CONFIG_PRINT, help='Print content of the current configuration')
    show_config_parser.add_argument('-dc', '--def-config', action='store_true', help='Show content of default config')

    create__config_parser = config_subparser.add_parser(
        ACTION_CONFIG_CREATE, help='Create configuration file', add_help=False)
    create__config_parser.add_argument("--overwrite", action="store_true", help="overwrite config file to default")


def _init_hostinfo_parser(common, subparsers):
    """
    Creates parsers for `hostinfo` command

    :param common: parent parser
    :param subparsers: sub-parser for hostinfo parser to be added to
    """

    hostinfo_parser = subparsers.add_parser(
        ACTION_HOSTINFO, parents=[common], description='Show host info', add_help=False)


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
    if hasattr(parsed, 'min_config') and parsed.min_config:
        config_options.append('min_config')
    if hasattr(parsed, 'config') and parsed.config:
        config_options.append('config')

    if len(config_options) > 1:
        parser.error('Conflicting config options: ' + str(config_options))
