"""
This is a command line interface for the `runcore` library.
"""

import sys

from runtools import runner
from runtools.runcli import cmd, cli, config
from runtools.runcli.cli import ACTION_SETUP
from runtools.runcore import util, paths
from runtools.runcore.common import RuntoolsException
from runtools.runcore.paths import ConfigFileNotFoundError

__version__ = "0.1.0"

from runtools.runcore.util import update_nested_dict

CONFIG_FILE = 'runcli.toml'


def main_cli():
    main(None)


def main(args):
    """Taro CLI app main function.

    Note: Configuration is set up before execution of all commands although not all commands require it.
          This practice increases safety (in regards with future extensions) and consistency.
          Performance impact is expected to be negligible.

    :param args: CLI arguments
    """
    try:
        run_app(args)
    except ConfigFileNotFoundError as e:
        print("User error: " + str(e), file=sys.stderr)
        if e.search_path:
            print("Run `setup config create` command to create the configuration file "
                  "or see `-dc` and `-mc` options to execute without config file", file=sys.stderr)
        exit(1)
    except RuntoolsException as e:
        print("User error: " + str(e), file=sys.stderr)
        exit(1)
    except KeyboardInterrupt:
        exit(130)


def run_app(args):
    args_parsed = cli.parse_args(args)

    if args_parsed.action == ACTION_SETUP:
        run_setup(args_parsed)
    else:
        configure_runner(args_parsed)
        run_command(args_parsed)


def run_setup(args):
    if args.setup_action == cli.ACTION_SETUP_CONFIG:
        run_config(args)


def run_config(args):
    if args.config_action == cli.ACTION_CONFIG_PRINT:
        if getattr(args, 'def_config', False):
            util.print_file(packed_config_path())
        else:
            util.print_file(paths.lookup_file_in_config_path(CONFIG_FILE))
    elif args.config_action == cli.ACTION_CONFIG_CREATE:
        created_file = paths.copy_config_to_search_path(config.__package__, CONFIG_FILE, args.overwrite)
        print("Created " + str(created_file))


def configure_runner(args):
    """Initialize runcli according to provided CLI arguments

    :param args: CLI arguments
    """
    if getattr(args, 'min_config', False):
        configuration = {}
    else:
        configuration = util.read_toml_file(resolve_config_path(args))
    update_nested_dict(configuration, util.split_params(args.set))  # Override config by `set` args
    runner.configure(**configuration)


def packed_config_path():
    return paths.package_config_path(config.__package__, CONFIG_FILE)


def resolve_config_path(args):
    """
    Resolve path to the configuration file based on provided CLI arguments.

    Args:
        args (Namespace): Parsed CLI arguments

    Returns:
        str: The configuration file path.
    """
    if getattr(args, 'config', None):
        return util.expand_user(args.config)

    if getattr(args, 'def_config', False):
        return packed_config_path()

    return paths.lookup_file_in_config_path(CONFIG_FILE)


def run_command(args_ns):
    cmd.run(args_ns)
