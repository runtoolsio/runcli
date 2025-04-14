"""
This is a command line interface for the `runjob` library.
"""
import tomllib
from pathlib import Path
from typing import Dict, Any

import sys

import runtools.runcore.util.files
from . import __version__, cmd, cli, config, log
from .cli import ACTION_CONFIG
from runtools.runcore import util, paths
from runtools.runcore.err import RuntoolsException
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.util.files import print_file
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
            print("Run `config create` command to create configuration file "
                  "or use `-dc` option to execute using default config", file=sys.stderr)
        exit(1)
    except RuntoolsException as e:
        print("User error: " + str(e), file=sys.stderr)
        exit(1)
    except KeyboardInterrupt:
        exit(130)


def run_app(args):
    args_parsed = cli.parse_args(args)

    if args_parsed.action == ACTION_CONFIG:
        run_config(args_parsed)
    else:
        configure_runner(args_parsed)
        run_command(args_parsed)


def run_config(args):
    if args.config_action == cli.ACTION_CONFIG_PRINT:
        if getattr(args, 'def_config', False):
            print_file(packed_config_path())
        else:
            print_file(paths.lookup_file_in_config_path(CONFIG_FILE))
    elif args.config_action == cli.ACTION_CONFIG_CREATE:
        if path := getattr(args, 'path'):
            created_file = runtools.runcore.util.files.copy_config_to_path(config.__package__, CONFIG_FILE, Path(path),
                                                                           args.overwrite)
        else:
            created_file = runtools.runcore.util.files.copy_config_to_search_path(config.__package__, CONFIG_FILE,
                                                                                  args.overwrite)
        print("Created " + str(created_file))


def read_toml_file(file_path: str) -> Dict[str, Any]:
    """
    Reads a TOML file and returns its contents as a dictionary.

    Args:
        file_path: The path to the TOML file.

    Returns:
        A dictionary representing the TOML data.

    Raises:
        FileNotFoundError: If the file_path does not exist.
        tomllib.TOMLDecodeError: If the file is not valid TOML.
        # Or potentially other IOErrors
    """
    with open(file_path, 'rb') as file:
        return tomllib.load(file)


def configure_runner(args):
    """Initialize runcli according to provided CLI arguments

    :param args: CLI arguments
    """
    if getattr(args, 'def_config', False):
        configuration = {}
    else:
        configuration = read_toml_file(resolve_config_path(args))
    update_nested_dict(configuration, util.split_params(args.set))  # Override config by `set` args
    print(configuration)
    # runner.configure(**configuration)
    # log.configure(True, 'debug')


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
