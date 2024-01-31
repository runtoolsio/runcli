"""
This is a command line interface for the `runcore` library.
"""

import os

import sys
from runtoolsio.taro import cmd, cli
from runtoolsio.taro.cli import ACTION_SETUP
from runtoolsio.taro.printer import print_styled
from runtoolsio.taro.theme import Theme

import runtoolsio.runcore.paths
from runtoolsio import runjob
from runtoolsio.runcore import util, paths
from runtoolsio.runcore.common import RuntoolsException, ConfigFileNotFoundError

__version__ = "0.1.0"


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
        print_styled((Theme.warning, "User error: "), ('', str(e)), file=sys.stderr)
        if e.search_path:
            print_styled(('', "Run `setup config create` command to create the configuration file "
                              "or see `-dc` and `-mc` options to execute without config file"), file=sys.stderr)
        exit(1)
    except RuntoolsException as e:
        print_styled((Theme.warning, "User error: "), ('', str(e)), file=sys.stderr)
        exit(1)


def run_app(args):
    args_parsed = cli.parse_args(args)

    if args_parsed.no_color or 'NO_COLOR' in os.environ or 'TARO_NO_COLOR' in os.environ:
        os.environ['PROMPT_TOOLKIT_COLOR_DEPTH'] = 'DEPTH_1_BIT'

    if args_parsed.action == ACTION_SETUP:
        run_setup(args_parsed)
    else:
        init_taro(args_parsed)
        run_command(args_parsed)


def run_setup(args):
    if args.setup_action == cli.ACTION_SETUP_CONFIG:
        run_config(args)


def run_config(args):
    if args.config_action == cli.ACTION_CONFIG_PRINT:
        if getattr(args, 'def_config', False):
            util.print_file(paths.default_config_file_path())
        else:
            util.print_file(paths.lookup_config_file())
    elif args.config_action == cli.ACTION_CONFIG_CREATE:
        created_file = runtoolsio.runcore.paths.copy_default_config_to_search_path(args.overwrite)
        print_styled((Theme.success, "Created "), ('', str(created_file)))


def init_taro(args):
    """Initialize taro according to provided CLI arguments

    :param args: CLI arguments
    """
    config_vars = util.split_params(args.set)  # Config variables and override values

    if getattr(args, 'config', None):
        runjob.load_config(args.config, **config_vars)
    elif getattr(args, 'def_config', False):
        runjob.load_defaults(**config_vars) # TODO
    elif getattr(args, 'min_config', False):
        runjob.configure(**config_vars)
    else:
        # taro.load_config(**config_vars)
        pass


def run_command(args_ns):
    try:
        cmd.run(args_ns)
    finally:
        runjob.close()
