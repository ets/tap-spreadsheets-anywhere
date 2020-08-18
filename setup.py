#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-spreadsheets-anywhere",
    version="0.1.0",
    description="Singer.io tap for extracting spreadsheet data from cloud storage",
    author="Eric Simmerman",
    url="https://github.com/ets/tap-spreadsheets-anywhere",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_spreadsheets_anywhere"],
    install_requires=[
        "singer-python>=5.0.12",
        'smart_open>=2.1',
        'voluptuous>=0.10.5',
        'xlrd',
    ],
    entry_points="""
    [console_scripts]
    tap-spreadsheets-anywhere=tap_spreadsheets_anywhere:main
    """,
    packages=["tap_spreadsheets_anywhere"],
    include_package_data=True,
)
