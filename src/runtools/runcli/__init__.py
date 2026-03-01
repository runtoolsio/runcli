"""
This is a command line interface for the `runjob` library.
"""
import logging

from rich.console import Console
from rich.text import Text

from runtools.runcore import util, env
from runtools.runcore.env import EnvironmentConfigUnion
from runtools.runcore.util.files import format_toml
from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceID
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.util import update_nested_dict
from runtools.runcore.util.parser import KVParser
from runtools.runjob.output import OutputSink, ParsingPreprocessor
from . import __version__, cmd, cli, log, job
from .cfg import CONFIG_FILE
from .cli import ACTION_CONFIG, ACTION_ENV

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
        logger.error(f"run_job_command_failed reason=[{e}]")
        console.print(Text().append("User error: ", style="bold red").append(str(e)))
        exit(1)
    except Exception:
        logger.exception("run_job_command_error")
        raise
    except KeyboardInterrupt:
        exit(130)


def run_app(args):
    args_parsed = cli.parse_args(args)

    if args_parsed.action == ACTION_CONFIG:
        run_config(args_parsed)
    elif args_parsed.action == ACTION_ENV:
        run_env(args_parsed)
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


def run_env(args):
    all_envs = getattr(args, 'all_envs', False)
    env_configs = env.get_env_configs().values() if all_envs else [env.get_env_config(getattr(args, 'env'))]
    for i, env_config in enumerate(env_configs):
        if all_envs:
            if i > 0:
                print()
            print(f"# Environment: {env_config.id}")
        print(format_toml(env_config.model_dump(mode='json')))
        if all_envs:
            print(f"{'─' * 30}")


def run_job(args):
    job_id = args.id or " ".join([args.command.removeprefix('./')] + args.arg)
    instance_id = InstanceID(job_id, getattr(args, 'run_id'))
    config = load_config_and_log_setup(instance_id, args)
    env_config = resolve_env_config(args, instance_id)
    program_args = [args.command] + args.arg
    checkpoint_id = getattr(args, 'checkpoint')

    # Build output sink from CLI args (with parsing if requested)
    output_sink = _build_output_sink(args)

    job.run(
        instance_id, env_config, program_args,
        bypass_output=args.bypass_output,
        no_output_storage=args.no_output_storage,
        excl=args.excl_run,
        excl_group=getattr(args, 'excl_group'),
        checkpoint_id=checkpoint_id,
        serial=args.serial,
        max_concurrent=args.max_concurrent,
        concurrency_group=getattr(args, 'concurrency_group', ),
        timeout=getattr(args, 'timeout', 0.0),
        timeout_signal=getattr(args, 'timeout_sig'),
        time_warning=getattr(args, 'time_warn'),
        output_warning=args.output_warn,
        output_sink=output_sink,
        tail_buffer_size=args.tail_buffer_size,
    )


def _build_output_sink(args):
    """Build output sink from CLI arguments, with parsing preprocessor if requested."""
    parsers = []

    if getattr(args, 'kv_filter', False):
        # Parse aliases from CLI: "count=completed" -> {'count': 'completed'}
        aliases = {}
        for alias_str in getattr(args, 'kv_alias', []):
            if '=' in alias_str:
                from_key, to_key = alias_str.split('=', 1)
                aliases[from_key.strip()] = to_key.strip()
        parsers.append(KVParser(aliases=aliases if aliases else None))

    # TODO: Add grok pattern support when implemented
    # for pattern in getattr(args, 'grok_pattern', []):
    #     parsers.append(GrokParser(pattern))

    if parsers:
        return OutputSink(ParsingPreprocessor(parsers))
    return None


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
        logger.warning(f"fallback_configuration_loaded instance=[{instance_id}] fallback_config=[{cfg_path}]")
        console.print(Text().append("Note: ", style="bold yellow")
                      .append(f"No config file found in search paths. Using built-in defaults. ")
                      .append("Run `config create` or use `-D/--def-config` to suppress this message."))

    return config


def configure_logging(config):
    log_config = config.get('log', {})
    log.configure(
        log_config.get('enabled', True),
        log_config.get('stdout', {}).get('level', 'warn'),
        log_config.get('file', {}).get('level', 'info'),
        log_config.get('file', {}).get('path', None),
    )


def resolve_env_config(args, instance_id) -> EnvironmentConfigUnion:
    if eid := getattr(args, 'env'):
        env_config = env.get_env_config(eid)
        logger.info(f"environment_config_loaded instance=[{instance_id}] env=[{env_config.id}]")
    else:
        env_config = env.get_default_env_config()
        logger.info(f"default_environment_config_loaded instance=[{instance_id}] env=[{env_config.id}]")

    return env_config
