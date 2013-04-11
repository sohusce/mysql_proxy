#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @ref http://code.google.com/p/python-iptools/
import re, sys

__all__ = ["validate_ip", "ip2long", "long2ip", "ip2hex", "hex2ip", "validate_cidr", "cidr2block", "IpRange", "IpRangeList"]

_PATTERN_IP = re.compile(r"^(\d{1,3}\.){0,3}\d{1,3}$")

def validate_ip(quad_ip):
    if isinstance(quad_ip, basestring):
        if _PATTERN_IP.match(quad_ip):
            quads = quad_ip.split(".")
            for quad in quads:
                if int(quad) > 255:
                    return False
            return True
    return False

_IP_SECTION = 4
def ip2long(quad_ip):
    if not validate_ip(quad_ip):
        return None
    quads = quad_ip.split(".")
    if len(quads) < _IP_SECTION:
        quads += [0 for i in range(_IP_SECTION - len(quads))]
    longip = 0
    for quad in quads:
        longip = (longip << 8) | int(quad)
    return longip

_MAX_IP = 0xffffffff
_MIN_IP = 0x00000000

def long2ip(longip):
    try:
        longip = int(longip)
    except ValueError, err:
        return None
    if longip > _MAX_IP or longip < _MIN_IP:
        raise ValueError("long ip must be between %d and %d." % (_MIN_IP, _MAX_IP))
    return "%d.%d.%d.%d" % (longip >> 24 & 0xff, longip >> 16 & 0xff, longip >> 8 & 0xff, longip & 0xff)

def ip2hex(addr):
    if not validate_ip(addr):
        return None
    return "%08x" % ip2long(addr)

def hex2ip(hex_str):
    try:
        longip = int(hex_str, 16)
    except ValueError:
        return None
    return long2ip(longip)

_PATTERN_CIDR = re.compile(r"^(\d{1,3}\.){0,3}\d{1,3}/\d{1,2}$")
_MAX_CIDR = 32
def validate_cidr(cidr_ip):
    if isinstance(cidr_ip, basestring):
        if _PATTERN_CIDR.match(cidr_ip):
            ip, base = cidr_ip.split("/")
            if validate_ip(ip) and int(base) <= _MAX_CIDR:
                return True
    return False

def cidr2block(cidr_ip):
    if not validate_cidr(cidr_ip):
        return None
    ip, base = cidr_ip.split("/")
    longip = ip2long(ip)
    shift = _MAX_CIDR - int(base)
    startIp = longip >> shift << shift
    mask = (1 << shift) - 1
    endIp = startIp | mask
    return (long2ip(startIp), long2ip(endIp))

class IpRange(object):
    def __init__(self, start, end=None):
        if end is None:
            if isinstance(start, tuple):
                start, end = start
            elif validate_cidr(start):
                start, end = cidr2block(start)
            else:
                end = start
        start, end = ip2long(start), ip2long(end)
        if start is None or end is None:
            raise TypeError("ip address is invalid")
        self.startIp = min(start, end)
        self.endIp = max(start, end)

    def __contains__(self, item):
        if isinstance(item, basestring):
            item = ip2long(item)
        if not item:
            raise TypeError("ip address is invalid")
        return self.startIp <= item <= self.endIp

    def __iter__(self):
        for ip in xrange(self.startIp, self.endIp + 1):
            yield long2ip(ip)

    def __repr__(self):
        return (long2ip(self.startIp), long2ip(self.endIp)).__repr__()

    def len(self):
        return self.endIp - self.startIp + 1
        
    def __len__(self):
        length = self.endIp - self.startIp + 1
        if length > sys.maxint:
            raise OverflowError("BIF len() excess sys.maxint (%d). Retrieving length by calling len() of %s instance" % (sys.maxint, self.__class__))
        else:
            return length

class IpRangeList(object):

    def __init__(self, *args):
        self.ips = tuple(map(IpRange, args))

    def __contains__(self, item):
        for ip in self.ips:
            if item in ip: return True
        return False

    def __iter__(self):
        for ipRange in self.ips:
            for ip in ipRange:
                yield ip

    def __repr__(self):
        return self.ips.__repr__()

    def len(self):
        return sum([ip.len() for ip in self.ips])

    def __len__(self):
        length = sum([ip.len() for ip in self.ips])
        if length > sys.maxint:
            raise OverflowError("BIF len() excess sys.maxint (%d). Retrieving length by calling len() of %s instance" % (sys.maxint, self.__class__))
        else:
            return length

