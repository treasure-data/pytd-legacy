#!/usr/bin/env python

import re
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

def read_version():
    with open("pytd/version.py") as f:
        m = re.match(r'__version__ = "([^\"]*)"', f.read())
        return m.group(1)

class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name="pytd",
    version=read_version(),
    description="Python wrappers for TD",
    author="Keisuke Nishida",
    author_email="keisuke.nishida@gmail.com",
    url="https://github.com/k24d/pytd",
    install_requires=open("requirements.txt").read().splitlines(),
    packages=find_packages(),
    cmdclass = {"test": PyTest},
    license="Apache License 2.0",
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development",
    ],
)
