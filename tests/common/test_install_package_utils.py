#  Copyright 2025 ThingsBoard
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from subprocess import CalledProcessError
from unittest import mock

import pytest
from pkg_resources import DistributionNotFound

from tb_mqtt_client.common.install_package_utils import install_package


@pytest.mark.parametrize("version", ["upgrade", "UPGRADE"])
def test_install_package_upgrade_success(version):
    with mock.patch("tb_mqtt_client.common.install_package_utils.check_call", return_value=0) as mock_call:
        result = install_package("somepkg", version)
        assert result is True
        mock_call.assert_called_once()


def test_install_package_upgrade_retry_success():
    # First fails with --user, then succeeds without
    with mock.patch("tb_mqtt_client.common.install_package_utils.check_call",
                    side_effect=[CalledProcessError(1, ""), 0]) as mock_call:
        result = install_package("somepkg", "upgrade")
        assert result is True
        assert mock_call.call_count == 2


def test_install_package_upgrade_all_fail():
    with mock.patch("tb_mqtt_client.common.install_package_utils.check_call",
                    side_effect=CalledProcessError(1, "")) as mock_call:
        result = install_package("somepkg", "upgrade")
        assert result is False
        assert mock_call.call_count == 2


def test_install_package_specific_version_already_installed():
    dist = mock.Mock()
    dist.version = "1.2.3"
    with mock.patch("tb_mqtt_client.common.install_package_utils.get_distribution", return_value=dist):
        result = install_package("somepkg", "1.2.3")
        assert result is True


@pytest.mark.parametrize("version_input,expected_arg", [
    ("1.2.4", "somepkg==1.2.4"),
    (">=1.2.4", "somepkg>=1.2.4"),
])
def test_install_package_specific_version_install_success(version_input, expected_arg):
    with mock.patch("tb_mqtt_client.common.install_package_utils.get_distribution", side_effect=DistributionNotFound):
        side_effects = [
            CalledProcessError(1, ["pip"]),
            0
        ]
        with mock.patch("tb_mqtt_client.common.install_package_utils.check_call",
                        side_effect=side_effects) as mock_call:
            result = install_package("somepkg", version_input)
            assert result is True
            calls = [str(call.args) for call in mock_call.call_args_list]
            assert any(expected_arg in args for args in calls)


def test_install_package_specific_version_retry_success():
    with mock.patch("tb_mqtt_client.common.install_package_utils.get_distribution", side_effect=DistributionNotFound):
        with mock.patch("tb_mqtt_client.common.install_package_utils.check_call",
                        side_effect=[CalledProcessError(1, ""), 0]) as mock_call:
            result = install_package("somepkg", "1.0.0")
            assert result is True
            assert mock_call.call_count == 2


def test_install_package_specific_version_all_fail():
    with mock.patch("tb_mqtt_client.common.install_package_utils.get_distribution", side_effect=DistributionNotFound):
        with mock.patch("tb_mqtt_client.common.install_package_utils.check_call",
                        side_effect=CalledProcessError(1, "")) as mock_call:
            result = install_package("somepkg", "1.0.0")
            assert result is False
            assert mock_call.call_count == 2
