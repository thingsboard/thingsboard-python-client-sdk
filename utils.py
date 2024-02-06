# Copyright 2024. ThingsBoard
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

from sys import executable
from subprocess import check_call, CalledProcessError


def install_package(package, version="upgrade"):
    result = False
    if version.lower() == "upgrade":
        try:
            result = check_call([executable, "-m", "pip", "install", package, "--upgrade", "--user"])
        except CalledProcessError:
            result = check_call([executable, "-m", "pip", "install", package, "--upgrade"])
    else:
        from pkg_resources import get_distribution
        current_package_version = None
        try:
            current_package_version = get_distribution(package)
        except Exception:
            pass
        if current_package_version is None or current_package_version != version:
            installation_sign = "==" if ">=" not in version else ""
            try:
                result = check_call(
                    [executable, "-m", "pip", "install", package + installation_sign + version, "--user"])
            except CalledProcessError:
                result = check_call([executable, "-m", "pip", "install", package + installation_sign + version])
    return result