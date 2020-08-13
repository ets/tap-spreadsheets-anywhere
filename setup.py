#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-smart-csv",
    version="0.1.0",
    description="Singer.io tap for extracting spreadsheet data from cloud storage",
    author="Eric Simmerman",
    url="https://github.com/ets/tap-smart-csv",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_smart_csv"],
    install_requires=[
        "singer-python>=5.0.12",
        'smart_open>=2.1',
        'voluptuous>=0.10.5',
        'xlrd',
    ],
    entry_points="""
    [console_scripts]
    tap-smart-csv=tap_smart_csv:main
    """,
    packages=["tap_smart_csv"],
    include_package_data=True,
)
