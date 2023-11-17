# Copyright 2023. ThingsBoard
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#  http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from os import path
from pkg_resources import DistributionNotFound, get_distribution
from re import split
from setuptools import setup


this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md')) as f:
    long_description = f.read()

VERSION = "1.5"

INSTALL_REQUIRES = ['tb-paho-mqtt-client>=1.6.3', 'requests>=2.31.0', 'simplejson']

# If none of packages in first installed, install second package
CHOOSE_INSTALL_REQUIRES = [
    (
        ('pymmh3'),
        'mmh3',
    )
]

def choose_requirement(mains, secondary):
    """If some version of main requirement installed, return main,
    else return secondary.

    """
    chosen = secondary
    for main in mains:
        try:
            name = split(r"[!<>=]", main)[0]
            get_distribution(name)
            chosen = main
            break
        except DistributionNotFound:
            pass

    return str(chosen)

def get_install_requirements(install_requires, choose_install_requires):
    for mains, secondary in choose_install_requires:
        install_requires.append(choose_requirement(mains, secondary))

    return install_requires

setup(
    version=VERSION,
    name="tb-mqtt-client",
    author="ThingsBoard",
    author_email="info@thingsboard.io",
    license="Apache Software License (Apache Software License 2.0)",
    description="ThingsBoard python client SDK",
    url="https://github.com/thingsboard/thingsboard-python-client-sdk",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    packages=["."],
    install_requires=get_install_requirements(INSTALL_REQUIRES, CHOOSE_INSTALL_REQUIRES),
    download_url='https://github.com/thingsboard/thingsboard-python-client-sdk/archive/%s.tar.gz' % VERSION)
