#!/usr/bin/env python
# -*- coding: utf-8 -*-
######################################################################
## Filename:      mysql_misc.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Fri Mar  2 15:05:01 2012
##                
## Description:   implementation of MySQL protocol
##                
######################################################################
import socket, sys
from struct import pack, unpack, unpack_from, calcsize

from helper import utils
from helper.constants import AuthConf

def pack_int8(num):
    return pack("B", num)

def pack_int16(num):
    return pack("H", num)

def pack_int32(num):
    return pack("I", num)

def pack_int64(num):
    return pack("Q", num)

def pack_fill():
    return "\x00"

def pack_filln(n):
    if n <= 0: return ""
    return "\x00" * n

def pack_string(cnt, null_terminated = False):
    if null_terminated:
        return pack(str(len(cnt)) + "sB", cnt, 0x00)
    else:
        return pack(str(len(cnt)) + "s", cnt)

def pack_header(buf_len, idx=1):
    buf_len_256 = buf_len / 256
    # utils.log(utils.cur(), buf_len, buf_len % 256, buf_len_256 % 256, buf_len_256 / 256, idx)
    return pack("4B", buf_len % 256, buf_len_256 % 256, buf_len_256 / 256, idx)

def pack_lenenc(length):
    """
    Length-Coded-Binary little-endian
    
    First Byte   Following   Description
    0-250        0           = value of first byte
    251          0           column value = NULL
                             only appropriate in a Row Data Packet
    252          2           = value of following 16-bit word
    253          3           = value of following 24-bit word
    254          8           = value of following 64-bit word
    """
    buf = ""
    if length < 251:
        buf  = pack("B", length)
    elif length < 2**16:
        buf  = pack("B", 252)
        buf += pack("2B", (length >> 0) & 0xff, (length >> 8) & 0xff)
    elif length < 2**24:
        buf  = pack("B", 253)
        buf += pack("3B", (length >> 0) & 0xff, (length >> 8) & 0xff, (length >> 16) & 0xff)
    else:
        buf  = pack("B", 254)
        buf += pack("4B", (length >> 0) & 0xff, (length >> 8) & 0xff, (length >> 16) & 0xff, (length >> 24) & 0xff)                
        buf += pack("4B", (length >> 32) & 0xff, (length >> 40) & 0xff, (length >> 48) & 0xff, (length >> 56) & 0xff)                
    return buf

def pack_string_lenenc(cnt):
    return pack_lenenc(len(cnt)) + pack_string(cnt)

def type_lenenc(packet, offset):
    """
    mysql packet type
    < 251(0xFB) normal result
    = 251(0xFB) NULL packet
    = 252(0xFC) 2 bytes
    = 253(0xFD) 3 bytes
    = 254(0xFE) 8 bytes or EOF
    = 254(0xFE) ERR
    """
    byte, offset = unpack_int8(packet, offset)
    lenenc_type = AuthConf.PACKET_TYPES["LENENC_ERR"]
    if byte < 251:
        lenenc_type = AuthConf.PACKET_TYPES["LENENC_INT"]
    elif byte == 251:
        lenenc_type = AuthConf.PACKET_TYPES["LENENC_NULL"]        
    elif byte == 252:
        lenenc_type = AuthConf.PACKET_TYPES["LENENC_INT"]        
    elif byte == 253:
        lenenc_type = AuthConf.PACKET_TYPES["LENENC_INT"]
    elif byte == 254:
        if offset + 8 > len(packet):
            lenenc_type = AuthConf.PACKET_TYPES["LENENC_EOF"]
        else:
            lenenc_type = AuthConf.PACKET_TYPES["LENENC_INT"]        
    else:
        lenenc_type = AuthConf.PACKET_TYPES["LENENC_ERR"]        
    return lenenc_type

def unpack_header_length(packet, offset=0):
    ret = 0
    if len(packet)-offset < 4: return (ret, offset)
    tmp, offset = unpack_int8(packet, offset)
    ret |= tmp << 0
    tmp, offset = unpack_int8(packet, offset)
    ret |= tmp << 8
    tmp, offset = unpack_int8(packet, offset)
    ret |= tmp << 16
    return (ret, offset)

def unpack_lenenc(packet, offset):
    """
    Length-Coded-Binary
    First Byte   Following   Description
    0-250        0           = value of first byte
    251          0           column value = NULL
                             only appropriate in a Row Data Packet
    252          2           = value of following 16-bit word
    253          3           = value of following 24-bit word
    254          8           = value of following 64-bit word
    """
    ret = 0
    byte, offset = unpack_int8(packet, offset)
    if byte < 251: # it's me
        ret = byte
    elif byte == 252: # 2 bytes
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 0
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 8
    elif byte == 253: # 3 bytes
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 0
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 8
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 16
    elif byte == 253: # 8 bytes
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 0
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 8
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 16
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 24
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 32
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 40
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 48
        tmp, offset = unpack_int8(packet, offset)
        ret |= tmp << 56
    return (ret, offset)


## unpack packet

def unpack_check(buf, idx, fmt):
    if len(buf) == 0 or idx >= len(buf) or len(buf[idx:]) < calcsize(fmt):
        return -1
    return 0

def find_packet_null(buf, idx = 0):
    if idx >= len(buf):
        return -1
    for i in range(len(buf) - idx):
        (data, ) = unpack_from("B", buf, idx + i)
        if data == 0x00:
            return (idx + i)
    return -1

def unpack_int8(buf, idx = 0):
    fmt = "B"
    err = unpack_check(buf, idx, fmt)
    if err == -1:
        return (-1, idx)
    (data, ) = unpack_from(fmt, buf, idx)
    return (data, idx + 1)

def unpack_int16(buf, idx = 0):
    fmt = "H"
    err = unpack_check(buf, idx, fmt)
    if err == -1:
        return (-1, idx)
    (data, ) = unpack_from(fmt, buf, idx)
    return (data, idx + 2)

def unpack_int32(buf, idx = 0):
    fmt = "I"
    err = unpack_check(buf, idx, fmt)
    if err == -1:
        return (-1, idx)
    (data, ) = unpack_from(fmt, buf, idx)
    return (data, idx + 4)

def unpack_int64(buf, idx = 0):
    fmt = "Q"
    err = unpack_check(buf, idx, fmt)
    if err == -1:
        return (-1, idx)
    (data, ) = unpack_from(fmt, buf, idx)
    return (data, idx + 8)

def unpack_string(buf, n, idx = 0):
    fmt = str(n) + "s"
    err = unpack_check(buf, idx, fmt)
    if err == -1:
        return (-1, idx)
    (data, ) = unpack_from(fmt, buf, idx)
    return (data, idx + n)

def unpack_string_null(buf, idx = 0):
    null_idx = find_packet_null(buf, idx)
    if null_idx == -1:
        return (-1, null_idx)
    (data, idx) = unpack_string(buf, null_idx - idx, idx)
    idx = skip_packetn(1, idx)
    return (data, idx)

def skip_header(idx = 0):
    return skip_packetn(4, idx)

def skip_packetn(n = 0, idx = 0):
    return (idx + n)

if __name__ == "__main__":
    # print pack_string("")
    print pack_string_lenenc("")
