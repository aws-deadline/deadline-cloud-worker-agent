# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from ipaddress import IPv6Address
from logging import getLogger
import socket

import psutil

from ..api_models import HostProperties, IpAddresses


_logger = getLogger(__name__)


def get_host_properties() -> HostProperties:
    """Enumerates and returns the host properties for supplying in the UpdateWorker API
    request

    Returns
    -------
    HostProperties:
        The host properties
    """
    host_properties = HostProperties()

    try:
        ip_addresses = _get_ip_addresses()
    except Exception as e:
        _logger.warning("Unable to determine IP addresses: %s", e)
    else:
        host_properties["ipAddresses"] = ip_addresses

    try:
        host_name = socket.gethostname()

    except Exception as e:
        _logger.warning("Unable to determine hostname: %s", e)
    else:
        host_properties["hostName"] = host_name

    return host_properties


def _get_ip_addresses() -> IpAddresses:
    """Enumerates the IP addresses of the host machine and returns them

    Returns
    -------
    IpAddresses:
        The IP addresses of the host machine
    """
    ipv6_addresses: set[str] = set()
    ipv4_addresses: set[str] = set()

    addresses_by_iface = psutil.net_if_addrs()
    for addresses in addresses_by_iface.values():
        for address in addresses:
            if address.family == socket.AddressFamily.AF_INET:
                ipv4_addresses.add(address.address)
            elif address.family == socket.AddressFamily.AF_INET6:
                # Parse the address
                addr = IPv6Address(address.address)
                # Strip the zone identifier (link-local info)
                # https://docs.python.org/3/library/ipaddress.html#conversion-to-strings-and-integers
                # > Note that IPv6 scoped addresses are converted to integers without scope zone ID.
                addr = IPv6Address(int(addr))
                # Convert to a fully expanded IPv6 string and make upper-case as expected by the API
                normalized_ip_addr = addr.exploded.upper()
                ipv6_addresses.add(normalized_ip_addr)
            else:
                _logger.debug(
                    "Skipped address %s of family %s", address.address, address.family.name
                )

    return IpAddresses(
        ipV4Addresses=list(ipv4_addresses),
        ipV6Addresses=list(ipv6_addresses),
    )
