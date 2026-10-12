"""Offline native builds shared by packaging, the CLI, and managed startup."""
import hashlib
import os
from pathlib import Path
import platform
import shlex
import shutil
import stat
import subprocess
import sysconfig
import tempfile


FLAGS = ('-O3', '-DNDEBUG', '-std=c++17', '-Wall', '-Wextra', '-Wpedantic')


class NativeBuildError(RuntimeError):
    pass


def source_path():
    package = Path(__file__).resolve().parent
    for source in (package / '_native' / 'swarmwire.cpp', package.parent / 'native' / 'swarmwire.cpp'):
        if source.is_file():
            return source
    raise NativeBuildError('Native source is missing; reinstall SwarmWire from a wheel or source distribution.')


def compile_binary(source, target):
    """Compile to a temporary file, then publish atomically. No shell or network."""
    if platform.system() not in ('Darwin', 'Linux'):
        raise NativeBuildError('Local native transport supports macOS/Linux; use the memory or NATS backend here.')
    command = shlex.split(os.environ.get('CXX', 'c++'))
    if not command or shutil.which(command[0]) is None:
        raise NativeBuildError('A C++17 compiler is needed for source builds. Install a platform wheel or set CXX.')
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.swarmwired-', dir=target.parent)
    os.close(fd)
    env = os.environ.copy()
    if platform.system() == 'Darwin':
        minimum = sysconfig.get_config_var('MACOSX_DEPLOYMENT_TARGET') or platform.mac_ver()[0]
        if int(minimum.split('.')[0]) >= 11:
            minimum = minimum.split('.')[0] + '.0'
        env.setdefault('MACOSX_DEPLOYMENT_TARGET', minimum)
    try:
        result = subprocess.run([*command, *FLAGS, str(source), '-o', temporary], env=env,
                                capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise NativeBuildError('Native compilation failed: ' + result.stderr[-2000:].strip())
        os.chmod(temporary, 0o700)
        os.replace(temporary, target)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise NativeBuildError('Native compilation could not finish: ' + str(exc)) from exc
    finally:
        Path(temporary).unlink(missing_ok=True)
    return target


def ensure_binary(*, binary=None, cache_dir=None):
    """Prefer an explicit or wheel-bundled binary; otherwise build once per user."""
    if platform.system() not in ('Darwin', 'Linux'):
        raise NativeBuildError('Local native transport supports macOS/Linux; use the memory or NATS backend here.')
    override = binary or os.environ.get('SWARMWIRE_BINARY')
    if override:
        candidate = Path(override).expanduser().resolve()
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            raise NativeBuildError('SWARMWIRE_BINARY must identify an executable broker.')
        return candidate
    bundled = Path(__file__).resolve().parent / '_native' / 'swarmwired'
    if bundled.is_file() and os.access(bundled, os.X_OK):
        return bundled
    source = source_path()
    cache = Path(cache_dir or os.environ.get('SWARMWIRE_CACHE_DIR') or
                 (Path(tempfile.gettempdir()) / ('swarmwire-build-' + str(os.getuid()))))
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = cache.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise NativeBuildError('The build cache must be a private directory owned by this user (mode 0700).')
    signature = hashlib.sha256(source.read_bytes() + Path(__file__).read_bytes() + repr((FLAGS, platform.system(), platform.machine(),
                               os.environ.get('CXX', 'c++'), os.environ.get('MACOSX_DEPLOYMENT_TARGET', ''))).encode()).hexdigest()[:24]
    target = cache / ('swarmwired-' + signature)
    import fcntl
    fd = os.open(cache / 'build.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if target.is_symlink():
            raise NativeBuildError('Refusing a symbolic link in the native build cache.')
        if not target.is_file() or not os.access(target, os.X_OK):
            compile_binary(source, target)
    return target
