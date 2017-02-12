# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

"""Document Foundation Pootle plugins
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

setup(
    name='th_pootle',
    version='0.0.1',
    description='Translate House Pootle extensions and plugins',
    long_description="Tools and integrations for Translate House",
    url='https://github.com/translate/th-pootle',
    author='Ryan Northey',
    author_email='ryan@synca.io',
    license='GPL3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: GPL3',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='th pootle tools extensions plugins',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    include_package_data=True,
    install_requires=['pootle'])
