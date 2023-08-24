# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from socket import AddressFamily
from typing import Generator, NamedTuple
from unittest.mock import MagicMock, patch

from psutil._common import snicaddr
from pytest import fixture, mark, param

from deadline_worker_agent.api_models import IpAddresses
from deadline_worker_agent.startup import host_properties as host_properties_mod


@fixture
def mod_logger_mock() -> Generator[MagicMock, None, None]:
    with patch.object(host_properties_mod, "_logger") as mod_logger_mock:
        yield mod_logger_mock


@fixture
def hostname() -> str:
    return "hostname"


@fixture(autouse=True)
def mock_socket_gethostname(hostname: str) -> Generator[MagicMock, None, None]:
    with patch.object(
        host_properties_mod.socket, "gethostname", return_value=hostname
    ) as mock_socket_gethostname:
        yield mock_socket_gethostname


class TestGetHostProperties:
    """Tests for get_host_properties() function"""

    def test_successful_ip_addresses(self) -> None:
        """Tests that _get_ip_addresses is called and when it successfully returns a value that
        the value is returned in the "ipAddresses" field of the returned HostProperties instance"""
        # GIVEN
        with patch.object(host_properties_mod, "_get_ip_addresses") as mock_get_ip_addresses:
            # WHEN
            host_properties = host_properties_mod.get_host_properties()

        # THEN
        mock_get_ip_addresses.assert_called_once_with()
        assert host_properties["ipAddresses"] is mock_get_ip_addresses.return_value

    def test_get_ip_addresses_exception(
        self,
        mod_logger_mock: MagicMock,
    ) -> None:
        """Tests that when _get_ip_addresses raises an exception, that the exception is logged at
        warning level and the returned HostProperties instance contains no "ipAddresses" field."""

        # GIVEN
        exception_msg = "an exception message"
        exception = Exception(exception_msg)
        mod_logger_warning: MagicMock = mod_logger_mock.warning
        with patch.object(
            host_properties_mod, "_get_ip_addresses", side_effect=exception
        ) as mock_get_ip_addresses:
            # WHEN
            host_properties = host_properties_mod.get_host_properties()

        # THEN
        mock_get_ip_addresses.assert_called_once_with()
        mod_logger_warning.assert_called_once_with(
            "Unable to determine IP addresses: %s", exception
        )
        assert "ipAddresses" not in host_properties

    @mark.parametrize(
        argnames="hostname",
        argvalues=(
            "hostname1",
            "hostname2",
        ),
    )
    def test_successful_hostname(
        self,
        hostname: str,
        mock_socket_gethostname: MagicMock,
    ) -> None:
        """Tests that socket.gethostname() is called and when it successfully returns a value that
        the value is returned in the "hostName" field of the returned HostProperties instance"""
        # WHEN
        host_properties = host_properties_mod.get_host_properties()

        # THEN
        mock_socket_gethostname.assert_called_once_with()
        assert host_properties["hostName"] == hostname

    def test_socket_gethostname_exception(
        self,
        mod_logger_mock: MagicMock,
        mock_socket_gethostname: MagicMock,
    ) -> None:
        """Tests that when socket.gethostname() raises an exception, that the exception is logged at
        warning level and the returned HostProperties instance contains no "hostname" field."""

        # GIVEN
        exception_msg = "an exception message"
        exception = Exception(exception_msg)
        mod_logger_warning: MagicMock = mod_logger_mock.warning
        mock_socket_gethostname.side_effect = exception

        # WHEN
        host_properties = host_properties_mod.get_host_properties()

        # THEN
        mock_socket_gethostname.assert_called_once_with()
        mod_logger_warning.assert_called_once_with("Unable to determine hostname: %s", exception)
        assert "hostName" not in host_properties


class DetectedAddress(NamedTuple):
    family: AddressFamily
    address: str


class TestGetIpAddresses:
    """Tests for _get_ip_addresses() function"""

    @fixture
    def net_if_addrs(self) -> dict[str, list[DetectedAddress]]:
        return {
            "eth0": [DetectedAddress(address="127.0.0.1", family=AddressFamily.AF_INET)],
        }

    @fixture(autouse=True)
    def mock_psutil_net_if_addrs(
        self,
        net_if_addrs: dict[str, list[DetectedAddress]],
    ) -> Generator[MagicMock, None, None]:
        with patch.object(host_properties_mod.psutil, "net_if_addrs") as mock_psutil_net_if_addrs:
            return_value: dict[str, list[snicaddr]] = {}
            for nic, addresses in net_if_addrs.items():
                return_value[nic] = [
                    snicaddr(
                        address=address.address,
                        family=address.family,
                        # These are ignored
                        broadcast=None,
                        netmask="255.255.255.0",
                        ptp=None,
                    )
                    for address in addresses
                ]
            mock_psutil_net_if_addrs.return_value = return_value
            yield mock_psutil_net_if_addrs

    @fixture
    def expected_ip_addresses(self) -> IpAddresses:
        return IpAddresses(
            ipV4Addresses=["127.0.0.1"],
            ipV6Addresses=[],
        )

    def test_calls_psutil_net_if_addrs(
        self,
        mock_psutil_net_if_addrs: MagicMock,
    ) -> None:
        """Tests that psutils.net_if_addresses() is called"""
        # WHEN
        host_properties_mod._get_ip_addresses()

        # THEN
        mock_psutil_net_if_addrs.assert_called_once_with()

    def test_simple(self, expected_ip_addresses: IpAddresses) -> None:
        """Tests that we get the expected IP addresses returned"""
        # WHEN
        ip_addresses = host_properties_mod._get_ip_addresses()

        # THEN
        assert ip_addresses == expected_ip_addresses

    @mark.parametrize(
        argnames=("net_if_addrs", "expected_ip_addresses"),
        argvalues=(
            param(
                {
                    "eth0": [DetectedAddress(address="127.0.0.1", family=AddressFamily.AF_INET)],
                    "wlan0": [DetectedAddress(address="127.0.0.1", family=AddressFamily.AF_INET)],
                },
                IpAddresses(
                    ipV4Addresses=["127.0.0.1"],
                    ipV6Addresses=[],
                ),
                id="ipv4-diff-iface",
            ),
            param(
                {
                    "eth0": [
                        DetectedAddress(address="127.0.0.1", family=AddressFamily.AF_INET),
                        DetectedAddress(address="127.0.0.1", family=AddressFamily.AF_INET),
                    ],
                },
                IpAddresses(
                    ipV4Addresses=["127.0.0.1"],
                    ipV6Addresses=[],
                ),
                id="ipv4-same-iface",
            ),
            param(
                {
                    "eth0": [
                        DetectedAddress(
                            address="0000:0000:0000:0000:0000:0000:0000:0001",
                            family=AddressFamily.AF_INET6,
                        )
                    ],
                    "wlan0": [
                        DetectedAddress(
                            address="0000:0000:0000:0000:0000:0000:0000:0001",
                            family=AddressFamily.AF_INET6,
                        )
                    ],
                },
                IpAddresses(
                    ipV4Addresses=[],
                    ipV6Addresses=["0000:0000:0000:0000:0000:0000:0000:0001"],
                ),
                id="ipv4-diff-iface",
            ),
            param(
                {
                    "eth0": [
                        DetectedAddress(
                            address="0000:0000:0000:0000:0000:0000:0000:0001",
                            family=AddressFamily.AF_INET6,
                        ),
                        DetectedAddress(
                            address="0000:0000:0000:0000:0000:0000:0000:0001",
                            family=AddressFamily.AF_INET6,
                        ),
                    ],
                },
                IpAddresses(
                    ipV4Addresses=[],
                    ipV6Addresses=["0000:0000:0000:0000:0000:0000:0000:0001"],
                ),
                id="ipv4-same-iface",
            ),
        ),
    )
    def test_deduplication(
        self,
        expected_ip_addresses: IpAddresses,
    ) -> None:
        """Tests that we get unique IP addresses returned"""
        # WHEN
        ip_addresses = host_properties_mod._get_ip_addresses()

        # THEN
        assert ip_addresses == expected_ip_addresses

    @mark.parametrize(
        argnames=("net_if_addrs", "expected_ip_addresses"),
        argvalues=(
            param(
                {
                    "eth0": [DetectedAddress(address="::1", family=AddressFamily.AF_INET6)],
                },
                IpAddresses(
                    ipV4Addresses=[],
                    ipV6Addresses=["0000:0000:0000:0000:0000:0000:0000:0001"],
                ),
                id="ipv4-diff-iface",
            ),
        ),
    )
    def test_ipv6_expansion(
        self,
        expected_ip_addresses: IpAddresses,
    ) -> None:
        """Tests that IPv6 compact addresses are expanded"""
        # WHEN
        ip_addresses = host_properties_mod._get_ip_addresses()

        # THEN
        assert ip_addresses == expected_ip_addresses

    @mark.parametrize(
        argnames=("net_if_addrs", "expected_ip_addresses"),
        argvalues=(
            param(
                {
                    "eth0": [DetectedAddress(address="::a1", family=AddressFamily.AF_INET6)],
                },
                IpAddresses(
                    ipV4Addresses=[],
                    ipV6Addresses=["0000:0000:0000:0000:0000:0000:0000:00A1"],
                ),
                id="ipv4-diff-iface",
            ),
        ),
    )
    def test_ipv6_uppercase(
        self,
        expected_ip_addresses: IpAddresses,
    ) -> None:
        """Tests that IPv6 addresses are normalized to upper case"""
        # WHEN
        ip_addresses = host_properties_mod._get_ip_addresses()

        # THEN
        assert ip_addresses == expected_ip_addresses

    @mark.parametrize(
        argnames=("net_if_addrs", "expected_ip_addresses"),
        argvalues=(
            param(
                {
                    "eth0": [
                        DetectedAddress(
                            address="0000:0000:0000:0000:0000:0000:0000:0001%wlan0",
                            family=AddressFamily.AF_INET6,
                        )
                    ],
                },
                IpAddresses(
                    ipV4Addresses=[],
                    ipV6Addresses=["0000:0000:0000:0000:0000:0000:0000:0001"],
                ),
                id="ipv4-diff-iface",
            ),
        ),
    )
    def test_ipv6_zone_id_removed(
        self,
        expected_ip_addresses: IpAddresses,
    ) -> None:
        """Tests that zone identifiers in IPv6 address are not returned"""
        # WHEN
        ip_addresses = host_properties_mod._get_ip_addresses()

        # THEN
        assert ip_addresses == expected_ip_addresses
