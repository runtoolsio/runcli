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

from runtools.runcore import connector
from runtools.runcore.env import (
    BUILTIN_LOCAL, EnvironmentEntry, EnvironmentKind, LocalEnvironmentConfig, create_environment, ensure_environment,
    lookup, open_env_db, save_env_config,
)
from runtools.runcore.run import TerminationStatus

ENV_ID = 'runcli_test'


@pytest.fixture
def home(monkeypatch):
    """XDG dirs isolated from the user's, with no environment registered — only the built-in `local`.

    The root is a short ``/tmp`` path — unix socket paths are length-limited (104 bytes on macOS).
    """
    root = Path(tempfile.mkdtemp(prefix='rc', dir='/tmp'))
    for var in ('XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_STATE_HOME', 'XDG_CACHE_HOME'):
        monkeypatch.setenv(var, str(root / var.lower()))
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def env(home):
    """A registered local environment with its own DB and socket root."""
    entry = EnvironmentEntry(id=ENV_ID, kind=EnvironmentKind.LOCAL, location=str(home / 'env.db'))
    create_environment(entry, LocalEnvironmentConfig(root_dir=home / 'sock'))
    return entry


def _run_job(*command, env_id=ENV_ID, **kwargs):
    return _runcli('job', *(('-e', env_id) if env_id else ()), *command, **kwargs)


def _runcli(*args, stderr=subprocess.STDOUT, **kwargs):
    argv = [sys.executable, '-m', 'runtools.runcli', *args]
    return subprocess.Popen(argv, env=os.environ.copy(), stdout=subprocess.PIPE, stderr=stderr, text=True, **kwargs)


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


def _local(home):
    """The built-in `local` environment, using the test's socket root rather than the user's."""
    local = lookup(BUILTIN_LOCAL)
    ensure_environment(local)
    save_env_config(local, LocalEnvironmentConfig(root_dir=home / 'sock'))
    return local


def test_without_env_option_runs_in_local_silently_when_no_other_environment_is_registered(home):
    local = _local(home)

    proc = _run_job('echo', 'hello', env_id=None, stderr=subprocess.PIPE)
    output, errors = proc.communicate(timeout=30)

    assert proc.returncode == 0, errors
    assert 'Warning' not in errors
    [run] = _history(local)
    assert run.job_id == 'echo hello'


def test_without_env_option_runs_in_local_and_warns_when_other_environments_are_registered(home, env):
    local = _local(home)

    proc = _run_job('echo', 'executed', env_id=None, stderr=subprocess.PIPE)
    output, errors = proc.communicate(timeout=30)

    assert proc.returncode == 0, errors
    assert 'executed' in output
    assert 'Warning' not in output  # stdout stays the job's own
    assert 'also registered: runcli_test' in ' '.join(errors.split())  # unwrapped: the console wraps long lines
    [run] = _history(local)
    assert run.job_id == 'echo executed'
    assert _history(env) == []


def test_explicit_env_option_does_not_warn(env):
    proc = _run_job('echo', 'executed', stderr=subprocess.PIPE)
    _, errors = proc.communicate(timeout=30)

    assert proc.returncode == 0, errors
    assert 'Warning' not in errors


def test_env_command_without_env_option_shows_local_and_warns_when_other_environments_are_registered(home, env):
    _local(home)

    proc = _runcli('env', stderr=subprocess.PIPE)
    output, errors = proc.communicate(timeout=30)

    assert proc.returncode == 0, errors
    assert 'root_dir' in output
    assert 'also registered: runcli_test' in ' '.join(errors.split())


def test_dependency_not_running_terminates_job_unsatisfied(env):
    proc = _run_job('-j', 'dependent', '--depends-on', 'absent', 'echo', 'executed')
    output, _ = proc.communicate(timeout=30)

    assert proc.returncode != 0, output
    assert 'executed' not in output
    [run] = _history(env)
    assert run.lifecycle.termination.status == TerminationStatus.UNSATISFIED


@pytest.mark.parametrize('pattern', ['dep', 'de*'])
def test_dependency_running_lets_job_run(env, pattern):
    dependency = _run_job('-j', 'dep', 'sleep', '60')
    try:
        _wait_for_active_run(env)
        proc = _run_job('-j', 'dependent', '--depends-on', pattern, 'echo', 'executed')
        output, _ = proc.communicate(timeout=30)
    finally:
        dependency.terminate()
        dependency.wait()

    assert proc.returncode == 0, output
    assert 'executed' in output


def test_approved_job_runs_program(env):
    proc = _run_job('-j', 'gated', '--approve', 'gate', 'echo', 'executed')
    try:
        active = _wait_for_active_run(env)
        with connector.connect(ENV_ID) as conn:
            _wait_for_phase_control(conn, active.instance_id, 'gate').approve()
        output, _ = proc.communicate(timeout=30)
    finally:
        proc.kill()

    assert proc.returncode == 0, output
    assert 'executed' in output
    [run] = _history(env)
    assert run.lifecycle.termination.status == TerminationStatus.COMPLETED


def test_await_satisfied_by_ended_run(env):
    _run_job('-j', 'first', '-r', 'b1', 'true').communicate(timeout=30)

    proc = _run_job('-j', 'second', '--await', 'first@b1', 'echo', 'executed')
    output, _ = proc.communicate(timeout=30)

    assert proc.returncode == 0, output
    assert 'executed' in output


def test_await_waits_for_running_job_to_end(env):
    first = _run_job('-j', 'first', '-r', 'b1', 'sleep', '2')
    try:
        _wait_for_active_run(env)
        proc = _run_job('-j', 'second', '--await', 'fir*@b1', 'echo', 'executed')
        output, _ = proc.communicate(timeout=30)
    finally:
        first.kill()

    assert proc.returncode == 0, output
    runs = {run.job_id: run.lifecycle for run in _history(env)}
    assert runs['second'].created_at < runs['first'].termination.terminated_at \
           < runs['second'].termination.terminated_at


def test_await_future_only_ignores_ended_run(env):
    _run_job('-j', 'first', '-r', 'b1', 'true').communicate(timeout=30)

    proc = _run_job('-j', 'second', '--await', 'first@b1', '--await-future-only', '--await-timeout', '1',
                    'echo', 'executed')
    output, _ = proc.communicate(timeout=30)

    assert proc.returncode != 0, output
    assert 'executed' not in output
    runs = {run.job_id: run.lifecycle for run in _history(env)}
    assert runs['second'].termination.status == TerminationStatus.TIMEOUT


def test_await_future_only_waits_for_running_job_to_end(env):
    first = _run_job('-j', 'first', '-r', 'b1', 'sleep', '2')
    try:
        _wait_for_active_run(env)
        proc = _run_job('-j', 'second', '--await', 'first@b1', '--await-future-only', 'echo', 'executed')
        output, _ = proc.communicate(timeout=30)
    finally:
        first.kill()

    assert proc.returncode == 0, output
    assert 'executed' in output


def test_await_timeout_terminates_job(env):
    proc = _run_job('-j', 'second', '--await', 'absent', '--await-timeout', '1', 'echo', 'executed')
    output, _ = proc.communicate(timeout=30)

    assert proc.returncode != 0, output
    assert 'executed' not in output
    [run] = _history(env)
    assert run.lifecycle.termination.status == TerminationStatus.TIMEOUT


def _wait_for_phase_control(conn, instance_id, phase_id, timeout=15):
    """The instance is persisted before its node serves it — retry until the control is reachable."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (inst := conn.get_instance(instance_id)) and (control := inst.find_phase_control_by_id(phase_id)):
            return control
        time.sleep(0.2)
    raise TimeoutError(f"Phase {phase_id} of {instance_id} never became controllable")


def _wait_for_active_run(entry, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with open_env_db(entry) as db:
            if runs := db.read_active_runs():
                return runs[0]
        time.sleep(0.2)
    raise TimeoutError("The run was never persisted as active")
