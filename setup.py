#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="clickhouse-mysql",

    # version should comply with PEP440
    version='0.0.20200128',

    description='MySQL to ClickHouse data migrator',
    long_description='MySQL to ClickHouse data migrator',

    # homepage
    url="https://github.com/altinity/clickhouse-mysql-data-reader",

    author="Vladislav Klimenko",
    author_email="sunsingerus@gmail.com",

    license="MIT",

    # see https://pypi.python.org/pypi?:action=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',

        'Topic :: Database',

        # should match license above
        'License :: OSI Approved :: MIT License',

        # supported Python versions
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
    ],

    # what does the project relate to?
    keywords='clickhouse mysql data migration',

    # list of packages to be included into project
    packages=find_packages(exclude=[
        'contrib',
        'docs',
        'tests',
    ]),

    # list of additional package data to be attached to packages
    package_data={
        'clickhouse_mysql': [
            # examples
            '../clickhouse_mysql_examples/*.sh',
            '../clickhouse_mysql_examples/*.sql',
            # converter examples
            '../clickhouse_mysql_converter/*.py',
            # init scripts
            '../clickhouse_mysql.init.d/*',
            # config files
            '../clickhouse_mysql.etc/*',
        ],
    },

    # run-time dependencies
    # these will be installed by pip
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        'mysqlclient',
        'mysql-replication',
        'clickhouse-driver',
        'configobj',
        'setuptools',
    ],

    # cross-platform support for pip to create the appropriate form of executable
    entry_points={
        'console_scripts': [
            # executable name=what to call
            'clickhouse-mysql=clickhouse_mysql:main',
        ],
    },

    #cmdclass={
    #    'develop': PostDevelopCommand,
    #    'install': PostInstallCommand,
    #},

    #    python_requires='>=3.3',
)
