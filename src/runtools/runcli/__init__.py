"""
This is a command line interface for the `runjob` library.
"""
import logging

import sys

from runtools.runcore import util, paths
from runtools.runcore.err import RuntoolsException
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.util import update_nested_dict
from . import __version__, cmd, cli, log
from .cfg import CONFIG_FILE
from .cli import ACTION_CONFIG

logger = logging.getLogger(__name__)


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
        run_command(args_parsed)


def run_config(args):
    if args.config_action == cli.ACTION_CONFIG_PRINT:
        if getattr(args, 'def_config', False):
            cfg.print_default_config_file()
        else:
            cfg.print_found_config_file()
    elif args.config_action == cli.ACTION_CONFIG_CREATE:
        print("Created " + str(cfg.create_config_file(getattr(args, 'path'), overwrite=args.overwrite)))


def run_command(args):
    cfg_found = True
    if getattr(args, 'def_config', False):
        config, cfg_path = cfg.read_default_configuration()
    else:
        if explicit_cfg := getattr(args, 'config', None):
            config, cfg_path = cfg.read_configuration(explicit_cfg)
        else:
            try:
                config, cfg_path = cfg.read_configuration()
            except ConfigFileNotFoundError as e:
                if getattr(args, 'config_required', False):
                    raise e
                else:
                    cfg_found = False
                    config, cfg_path = cfg.read_default_configuration()
    update_nested_dict(config, util.split_params(args.set))  # Override config by `set` args

    log_config = config.get('log', {})
    log.configure(
        log_config.get('enabled', True),
        log_config.get('stdout', {}).get('level', 'warn'),
        log_config.get('file', {}).get('level', 'info'),
        log_config.get('file', {}).get('path', None),
    )
    if cfg_found:
        logger.info(f"[configuration_loaded] source=[{cfg_path}]")
    else:
        logger.warning(f"[fallback_configuration_loaded] fallback_source=[{cfg_path}] reason=[config_file_not_found]")
