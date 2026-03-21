"""
This is a command line interface for the `runjob` library.
"""
import logging

from rich.console import Console
from rich.text import Text

from runtools.runcore import util, env
from runtools.runcore.env import lookup, load_env_config, BUILTIN_LOCAL
from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceID, DuplicateStrategy
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.run import JobCompletionError
from runtools.runcore.util import update_nested_dict
from runtools.runcore.util.files import format_toml
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
    except JobCompletionError as e:
        logger.warning(f"run_unsuccessful instance=[{e.instance_id}] termination=[{e.termination}]")
        exit(1)
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
    if all_envs:
        registry = env.load_registry()
        env_configs = [load_env_config(entry) for entry in registry.values()]
    else:
        entry = lookup(getattr(args, 'env', None) or BUILTIN_LOCAL)
        env_configs = [load_env_config(entry)]
    for i, env_config in enumerate(env_configs):
        if all_envs:
            if i > 0:
                print()
            print(f"# Environment: {env_config.id}")
        print(format_toml(env_config.model_dump(mode='json')))
        if all_envs:
            print(f"{'─' * 30}")


def _resolve_duplicate_strategy(args):
    if getattr(args, 'allow_duplicate', False):
        return DuplicateStrategy.ALLOW
    if getattr(args, 'suppress_duplicate', False):
        return DuplicateStrategy.SUPPRESS
    return DuplicateStrategy.DISALLOW


def run_job(args):
    job_id = args.id or " ".join([args.command.removeprefix('./')] + args.arg)
    run_id = getattr(args, 'run_id')
    config = load_config_and_log_setup(InstanceID(job_id, run_id), args)
    program_args = [args.command] + args.arg
    checkpoint_id = getattr(args, 'checkpoint')

    # Build output sink from CLI args (with parsing if requested)
    output_sink = _build_output_sink(args)

    job.run(
        job_id, run_id, getattr(args, 'env', None), program_args,
        bypass_output=args.bypass_output,
        disable_output=tuple(args.disable_output),
        excl=args.excl_run,
        excl_group=getattr(args, 'excl_group'),
        checkpoint_id=checkpoint_id,
        serial=args.serial,
        max_concurrent=args.max_concurrent,
        concurrency_group=getattr(args, 'concurrency_group', None),
        timeout=getattr(args, 'timeout', 0.0),
        timeout_signal=getattr(args, 'timeout_sig'),
        time_warning=getattr(args, 'time_warn'),
        output_warning=args.output_warn,
        output_sink=output_sink,
        tail_buffer_size=args.tail_buffer_size,
        duplicate_strategy=_resolve_duplicate_strategy(args),
    )


def _build_output_sink(args):
    """Build output sink with KV parsing. Parsing is on by default, use --no-kv to disable."""
    if getattr(args, 'no_kv', False):
        return None

    aliases = {}
    for alias_str in getattr(args, 'kv_alias', []):
        if '=' in alias_str:
            from_key, to_key = alias_str.split('=', 1)
            aliases[from_key.strip()] = to_key.strip()

    return OutputSink(ParsingPreprocessor([KVParser(aliases=aliases if aliases else None)]))


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
        logger.debug(f"fallback_configuration_loaded instance=[{instance_id}] fallback_config=[{cfg_path}]")

    return config


def configure_logging(config):
    log_config = config.get('log', {})
    log.configure(
        log_config.get('enabled', True),
        log_config.get('stdout', {}).get('level', 'warn'),
        log_config.get('file', {}).get('level', 'info'),
        log_config.get('file', {}).get('path', None),
    )


