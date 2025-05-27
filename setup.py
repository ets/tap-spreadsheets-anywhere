#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-spreadsheets-anywhere",
    version="0.2.0",
    description="Singer.io tap for extracting spreadsheet data from cloud storage",
    author="Eric Simmerman",
    url="https://github.com/ets/tap-spreadsheets-anywhere",
    py_modules=["tap_spreadsheets_anywhere"],
    install_requires=[
        'azure-storage-blob>=12.14.0',
        'boto3>=1.15.5',
        'google-cloud-storage>=2.7.0',
        'openpyxl',
        'paramiko',
        'protobuf>=4.21.12',
        'pyarrow>=5.0.0',
        'pyexcel>=0.7',
        'pyexcel-ods3>=0.6',
        'singer-python>=5.0.12',
        'smart_open>=2.1',
        'voluptuous>=0.10.5',
        'xlrd'
    ],
    packages=["tap_spreadsheets_anywhere"],
    include_package_data=True,
    tests_require=[
        'pytest'
    ]
)
