"""
This is a command line interface for the `runjob` library.
"""
import logging

from rich.console import Console
from rich.text import Text

from runtools.runcore import util, env
from runtools.runcore.env import DEFAULT_LOCAL_ENVIRONMENT, EnvironmentNotFoundError, EnvironmentConfigUnion
from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceID
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.util import update_nested_dict
from . import __version__, cmd, cli, log, job
from .cfg import CONFIG_FILE
from .cli import ACTION_CONFIG

logger = logging.getLogger(__name__)

console = Console(stderr=True)


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
    except RuntoolsException as e:
        # TODO print to stderr
        console.print(Text().append("User error: ", style="bold red").append(str(e)))
        exit(1)
    except KeyboardInterrupt:
        exit(130)


def run_app(args):
    args_parsed = cli.parse_args(args)

    if args_parsed.action == ACTION_CONFIG:
        run_config(args_parsed)
    else:
        run_job(args_parsed)


def run_config(args):
    if args.config_action == cli.ACTION_CONFIG_PRINT:
        if getattr(args, 'def_config', False):
            cfg.print_default_config_file()
        else:
            cfg.print_found_config_file()
    elif args.config_action == cli.ACTION_CONFIG_CREATE:
        print("Created " + str(cfg.create_config_file(getattr(args, 'path'), overwrite=args.overwrite)))


def run_job(args):
    job_id = args.id or " ".join([args.command.removeprefix('./')] + args.arg)
    instance_id = InstanceID(job_id, getattr(args, 'run_id'))
    config = load_config_and_log_setup(instance_id, args)
    env_config = get_env_config(args, config, instance_id)
    program_args = [args.command] + args.arg
    approve_id = getattr(args, 'approve')

    job.run(instance_id, env_config, program_args, excl=args.exclusive_run, approve_id=approve_id)


def load_config_and_log_setup(instance_id, args):
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
    configure_logging(config)

    if cfg_found:
        logger.info(f"configuration_loaded instance=[{instance_id}] source=[{cfg_path}]")
    else:
        logger.warning(f"fallback_configuration_loaded instance=[{instance_id}] fallback_source=[{cfg_path}] reason=[config_file_not_found]")

    return config


def configure_logging(config):
    log_config = config.get('log', {})
    log.configure(
        log_config.get('enabled', True),
        log_config.get('stdout', {}).get('level', 'warn'),
        log_config.get('file', {}).get('level', 'info'),
        log_config.get('file', {}).get('path', None),
    )


def get_env_config(args, config, instance_id) -> EnvironmentConfigUnion:
    def_env_id = config.get('environments', {'default': DEFAULT_LOCAL_ENVIRONMENT}).get('default', DEFAULT_LOCAL_ENVIRONMENT)
    env_id = getattr(args, 'env') or def_env_id
    try:
        env_config, env_config_path = env.load_env_config(env_id)
        logger.info(f"environment_config_loaded instance=[{instance_id}] env=[{env_id}] path=[{env_config_path}]")
    except (ConfigFileNotFoundError, EnvironmentNotFoundError) as e:
        if getattr(args, 'config_required', False) or env_id != DEFAULT_LOCAL_ENVIRONMENT:
            logger.error(f"[environment_config_not_found] instance=[{instance_id}] env=[{env_id}]")
            raise e
        env_config, env_config_path = env.load_env_default_config(env_id)
        logger.info(f"environment_default_config_loaded instance=[{instance_id}] env=[{env_id}] path=[{env_config_path}] "
                    f"reason=[No config for `{env_id}` env found]")

    return env.env_config_from_dict(env_config)
