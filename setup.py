"""Build platform wheels with the broker included; installation needs no daemon."""
from pathlib import Path
import runpy
import shutil
from setuptools import setup
from setuptools.command.build_py import build_py
from wheel.bdist_wheel import bdist_wheel

ROOT = Path(__file__).resolve().parent
build_native = runpy.run_path(str(ROOT / 'swarmwire' / '_build.py'))['compile_binary']

class BuildWithBroker(build_py):
    def run(self):
        super().run()
        destination = Path(self.build_lib) / 'swarmwire' / '_native'
        destination.mkdir(parents=True, exist_ok=True)
        source = ROOT / 'native' / 'swarmwire.cpp'
        shutil.copyfile(source, destination / 'swarmwire.cpp')
        build_native(source, destination / 'swarmwired')
        (destination / 'swarmwired').chmod(0o755)

    def get_outputs(self, include_bytecode=1):
        extra = Path(self.build_lib) / 'swarmwire' / '_native'
        return super().get_outputs(include_bytecode) + [str(extra / 'swarmwire.cpp'), str(extra / 'swarmwired')]

class PlatformWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):
        _, _, platform_tag = super().get_tag()
        return 'py3', 'none', platform_tag

setup(cmdclass={'build_py': BuildWithBroker, 'bdist_wheel': PlatformWheel})
