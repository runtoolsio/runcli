"""`run job` end to end: a real runcli process against an isolated `local` environment."""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from runtools.runcore.env import (
    EnvironmentEntry, EnvironmentKind, LocalEnvironmentConfig, create_environment, open_env_db,
)
from runtools.runcore.run import TerminationStatus

ENV_ID = 'runcli_test'


@pytest.fixture
def env(monkeypatch):
    """A registered local environment isolated from the user's: own XDG dirs, DB and socket root.

    The root is a short ``/tmp`` path — unix socket paths are length-limited (104 bytes on macOS).
    """
    root = Path(tempfile.mkdtemp(prefix='rc', dir='/tmp'))
    for var in ('XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_STATE_HOME', 'XDG_CACHE_HOME'):
        monkeypatch.setenv(var, str(root / var.lower()))
    entry = EnvironmentEntry(id=ENV_ID, kind=EnvironmentKind.LOCAL, location=str(root / 'env.db'))
    create_environment(entry, LocalEnvironmentConfig(root_dir=root / 'sock'))
    yield entry
    shutil.rmtree(root, ignore_errors=True)


def _run_job(*command, **kwargs):
    argv = [sys.executable, '-m', 'runtools.runcli', 'job', '-e', ENV_ID, *command]
    return subprocess.Popen(argv, env=os.environ.copy(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, **kwargs)


def _history(entry):
    with open_env_db(entry) as db:
        return db.read_runs()


def test_completed_command_is_recorded(env):
    proc = _run_job('echo', 'hello')
    output, _ = proc.communicate(timeout=30)

    assert proc.returncode == 0, output
    assert 'hello' in output
    [run] = _history(env)
    assert run.job_id == 'echo hello'
    assert run.lifecycle.termination.status == TerminationStatus.COMPLETED


def test_failing_command_is_recorded_and_exits_non_zero(env):
    proc = _run_job('false')
    output, _ = proc.communicate(timeout=30)

    assert proc.returncode != 0, output
    [run] = _history(env)
    assert run.lifecycle.termination.status == TerminationStatus.FAILED


def test_killed_run_stays_active_until_marked_lost(env):
    proc = _run_job('sleep', '60')
    try:
        active = _wait_for_active_run(env)
    finally:
        os.kill(proc.pid, signal.SIGKILL)  # no finalization: the run stays non-ended
        proc.wait()

    with open_env_db(env) as db:
        [run] = db.read_active_runs()  # storage still lists it — the local live view no longer would
        assert run.instance_id == active.instance_id
        assert db.mark_lost_runs([run.instance_id], stale_after=0) == [run.instance_id]
        [ended] = db.read_runs()
    assert ended.lifecycle.termination.status == TerminationStatus.LOST


def _wait_for_active_run(entry, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with open_env_db(entry) as db:
            if runs := db.read_active_runs():
                return runs[0]
        time.sleep(0.2)
    raise TimeoutError("The run was never persisted as active")
