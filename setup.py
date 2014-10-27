#!/usr/bin/python
#
#    Copyright Reliance Jio Infocomm, Ltd.
#    Author: Soren Hansen <Soren.Hansen@ril.com>
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

from setuptools import setup, find_packages

setup(
    name='jiocloud',
    version='0.4',
    description='Various Python utilities used for JioCloud',
    author='Soren Hansen',
    author_email='Soren.Hansen@ril.com',
    url='http://github.com/JioCloud/python-jiocloud',
    packages=find_packages(),
    include_package_data=True,
    license='Apache 2.0',
    keywords='etcd openstack cloud',
    install_requires=['python-etcd', 'IPy', 'mock', 'python-novaclient', 'PyYAML', 'netifaces', 'consulate', 'tornado'],
)
