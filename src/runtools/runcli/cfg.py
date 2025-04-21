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
from runtools.runcore.paths import expand_user, ConfigFileNotFoundError
from runtools.runcore.util.files import print_file, copy_config_to_path, copy_config_to_search_path

CONFIG_FILE = 'runcli.toml'


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
    path = _packed_config_path()
    return _read_toml_file(path), path


def read_configuration(explicit_path=None):
    if explicit_path:
        path = expand_user(explicit_path)
    else:
        path = paths.lookup_file_in_config_path(CONFIG_FILE)

    try:
        return _read_toml_file(path), path
    except FileNotFoundError:
        raise ConfigFileNotFoundError(explicit_path or path)
