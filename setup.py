#!/usr/bin/env python

import re
from setuptools import setup, find_packages

def read_version():
    with open("pytd/version.py") as f:
        m = re.match(r'__version__ = "([^\"]*)"', f.read())
        return m.group(1)

setup(
    name="pytd",
    version=read_version(),
    description="Python wrappers for TD",
    author="Keisuke Nishida",
    author_email="keisuke.nishida@gmail.com",
    url="https://github.com/k24d/pytd",
    install_requires=open("requirements.txt").read().splitlines(),
    packages=find_packages(),
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
