"""
Config util
For more information read: https://github.com/tarotools/taro-core/blob/master/docs/CONFIG.md
"""
import logging
import tomllib
from pathlib import Path
from typing import Dict, Any

from runtools.runcli import config
from runtools.runcore import paths
from runtools.runcore.err import RuntoolsException
from runtools.runcore.paths import expand_user, ConfigFileNotFoundError
from runtools.runcore.util.files import print_file, copy_config_to_path, copy_config_to_search_path

CONFIG_FILE = 'runcli.toml'


def validate_log_level(val):
    if val is None:
        return None

    str_val = str(val).upper()
    if str_val not in logging.getLevelNamesMapping():
        raise InvalidConfigField(f"Invalid log level value `{val}`, valid are: {logging.getLevelNamesMapping().keys()}")

    return str_val


def print_default_config_file():
    print_file(_packed_config_path())


def _packed_config_path():
    return paths.package_config_path(config.__package__, CONFIG_FILE)


def print_found_config_file():
    print_file(paths.lookup_file_in_config_path(CONFIG_FILE))


def create_config_file(path=None, *, overwrite=False):
    if path:
        return copy_config_to_path(config.__package__, CONFIG_FILE, Path(path), overwrite)
    else:
        return copy_config_to_search_path(config.__package__, CONFIG_FILE, overwrite)


def _read_toml_file(file_path) -> Dict[str, Any]:
    """
    Reads a TOML file and returns its contents as a dictionary.

    Args:
        file_path (str|Path): The path to the TOML file.

    Returns:
        A dictionary representing the TOML data.

    Raises:
        FileNotFoundError: If the file_path does not exist.
        tomllib.TOMLDecodeError: If the file is not valid TOML.
    """
    with open(file_path, 'rb') as file:
        return tomllib.load(file)


def read_default_configuration():
    return _read_toml_file(_packed_config_path())


def read_configuration(path=None, *, default_for_missing=False):
    if path:
        path = expand_user(path)
    else:
        try:
            path = paths.lookup_file_in_config_path(CONFIG_FILE)
        except ConfigFileNotFoundError as e:
            if default_for_missing:
                path = _packed_config_path()
            else:
                raise e

    return _read_toml_file(path)
