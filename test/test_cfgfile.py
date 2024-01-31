"""
Tests :mod:`cfgfile` module
Description: Config related tests
"""

import pytest
import runtools.runcore.cfg

from runtools.runcore import cfg
from runtools.runcore import paths
from runtools.runcore.test.testutil import create_test_config, remove_test_config
from src.runtools.runcli.log import LogMode


@pytest.fixture(autouse=True)
def remove_config_if_created():
    yield
    remove_test_config()


def test_defaults():
    create_test_config(dict())
    runtools.runcore.cfg.load_from_file()
    assert cfg.log_mode == LogMode.PROPAGATE
    assert cfg.log_stdout_level == 'warn'
    assert cfg.log_file_level == 'off'
    assert cfg.log_file_path is None
    assert cfg.persistence_enabled
    assert cfg.persistence_type == 'sqlite'
    assert not cfg.plugins_enabled
    assert cfg.plugins_load == ()


def test_default_config():
    runtools.runcore.cfg.load_from_file(paths.default_config_file_path())
    assert cfg.log_mode == LogMode.DISABLED
    assert cfg.log_stdout_level == 'warn'
    assert cfg.log_file_level == 'info'
    assert cfg.log_file_path is None
    assert cfg.persistence_enabled
    assert cfg.persistence_type == 'sqlite'
    assert cfg.persistence_max_records == -1
    assert cfg.plugins_load == ()


def test_field_with_underscore():
    create_test_config({"persistence_max_records": 200})
    runtools.runcore.cfg.load_from_file()
    assert cfg.persistence_max_records == 200


def test_plugins_array():
    create_test_config({"plugins": {"load": ["p1", "p2"]}})
    runtools.runcore.cfg.load_from_file()
    assert cfg.plugins_load == ("p1", "p2")


def test_str_type_error():
    create_test_config({"log": {"stdout": {"level": 3}}})  # Non-str value
    with pytest.raises(TypeError):
        runtools.runcore.cfg.load_from_file()


def test_bool_type_error():
    create_test_config({"log": {"timing": "hello"}})  # Non-bool value
    with pytest.raises(ValueError):
        runtools.runcore.cfg.load_from_file()
