"""
Config util
For more information read: https://github.com/tarotools/taro-core/blob/master/docs/CONFIG.md
"""
from pathlib import Path

from runtools.runcli import config
from runtools.runcore import paths
from runtools.runcore.paths import expand_user, ConfigFileNotFoundError
from runtools.runcore.util.files import print_file, copy_config_to_path, copy_config_to_search_path, read_toml_file

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


def read_default_configuration():
    path = _packed_config_path()
    return read_toml_file(path), path


def read_configuration(explicit_path=None):
    if explicit_path:
        path = expand_user(explicit_path)
    else:
        path = paths.lookup_file_in_config_path(CONFIG_FILE)

    try:
        return read_toml_file(path), path
    except FileNotFoundError:
        raise ConfigFileNotFoundError(explicit_path or path)
