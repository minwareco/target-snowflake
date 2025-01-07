#!/usr/bin/env python

from os import path

from setuptools import setup, find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='target-snowflake',
    url='https://github.com/minwareco/target-snowflake',
    author='minware',
    version="0.2.5",
    description='Singer.io target for loading data into Snowflake',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_snowflake'],
    install_requires=[
        'singer-python==5.9.0',
        'target_postgres@git+https://github.com/minwareco/target-postgres.git@19aae4840a0c5d2ac4cf30848f739f3c23df0a66',
        'target-redshift==0.2.4',
        'botocore<1.13.0,>=1.12.253',
        'snowflake-connector-python==3.9.1',
    ],
    setup_requires=[
        "pytest-runner"
    ],
    extras_require={
        'tests': [
            "Faker==19.13.0",
            "pytest==7.4.3"
        ]},
    entry_points='''
      [console_scripts]
      target-snowflake=target_snowflake:cli
    ''',
    packages=find_packages()
)
