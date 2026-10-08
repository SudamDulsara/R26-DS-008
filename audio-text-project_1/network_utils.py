# =====================================================
# network_utils.py
# =====================================================

import socket

_original_getaddrinfo = socket.getaddrinfo


def _ipv4_preferred_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    results = _original_getaddrinfo(host, port, family, type, proto, flags)

    ipv4_results = [r for r in results if r[0] == socket.AF_INET]

    return ipv4_results if ipv4_results else results


def force_ipv4():
    """
    Some networks advertise IPv6 routes that are actually
    unreachable (packets are silently dropped), causing
    httplib2/googleapiclient to hang until a raw socket
    timeout instead of falling back to IPv4. Forcing IPv4
    resolution avoids that hang.
    """
    socket.getaddrinfo = _ipv4_preferred_getaddrinfo
