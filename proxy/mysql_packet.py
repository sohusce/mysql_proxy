#!/usr/bin/evn python
# -*- coding: utf-8 -*-
######################################################################
## Filename:      mysql_packet.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Fri Mar  2 15:05:01 2012
##                
## Description:   analyze packet of handshake-authenticate-request-quit
##                
######################################################################
from math import floor
import socket, sys, hashlib

from mysql_misc import *
from helper import utils
from helper.constants import AuthConf, ServerCapability, Command

def pack_err_packet(errno, sqlstate, message, idx = 1):
    if not len(sqlstate) == 6:
        return ""
    buf = pack_int8(0xff)
    buf += pack_int16(errno)
    buf += pack_string(sqlstate)
    buf += pack_string(message)
    header = pack_header(len(buf), idx)
    # with open("/tmp/err", "ab") as f:
    #     f.write(header + buf)
    return header + buf

def pack_ok_packet(rows=0, insertid=0, server_status=0, warns=0, msg="", idx=1):
    """
    VERSION 4.1
    Bytes                       Name
    -----                       ----
    1   (Length Coded Binary)   field_count, always = 0
    1-9 (Length Coded Binary)   affected_rows
    1-9 (Length Coded Binary)   insert_id
    2                           server_status
    2                           warning_count
    n   (until end of packet)   message
    """
    buf = "\x00"
    buf += pack_lenenc(rows)
    buf += pack_lenenc(insertid)
    buf += pack_int16(server_status)
    buf += pack_int16(warns)
    buf += pack_string(msg)
    header = pack_header(len(buf), idx)
    return header + buf
    

def pack_client_flags(num = 0x0003a68d):
    return pack_int32(num)

def pack_max_packet_size(num = 16777216):
    return pack_int32(num)

# default charset utf8
def pack_charset_number(num = 0x21):
    return pack_int8(num)

# def pack_charset_number(num = 0x08):
#     return pack_int8(num)

class RandSt(object):

    def __init__(self):
        max_value = 0
        max_value_dbl = 0.0
        seed1 = 0
        seed2 = 0

    def reset(self):
        self.max_value = 0
        self.max_value_dbl = 0.0
        self.seed1 = 0
        self.seed2 = 0

"""
libmysql/password.c randominit 78
"""
def randominit(rand_st, seed1, seed2):
    if not isinstance(rand_st, RandSt): raise Exception("not a RandSt instance")
    rand_st.max_value = 0x3FFFFFFFL
    rand_st.max_value_dbl = rand_st.max_value * 1.0
    rand_st.seed1 = seed1 % rand_st.max_value
    rand_st.seed2 = seed2 % rand_st.max_value

"""
libmysql/password.c my_rnd 78
"""
def my_rnd(rand_st):
    if not isinstance(rand_st, RandSt): raise Exception("not a RandSt instance")
    rand_st.seed1 = (rand_st.seed1 * 3 + rand_st.seed2) % rand_st.max_value
    rand_st.seed2 = (rand_st.seed1 + rand_st.seed2 + 33) % rand_st.max_value
    return rand_st.seed1 / rand_st.max_value_dbl

def ulong(num):
    return num & AuthConf.MASK_UNSIGNED_LONG

"""
libmysql/password.c hash_password 117
"""
def hash_password(cnt, cntlen):
    nr  = 1345345333
    nr2 = 0x12345671
    add = 7
    for i in range(cntlen):
        if cnt[i] == " " or cnt[i] == "\t": continue
        (tmp, _) = unpack_int8(cnt, i)
        nr ^= ulong(((nr & 63) + add) * tmp + (nr << 8))
        nr2 += ulong(nr2 << 8) ^ nr
        nr2 = ulong(nr2)
        add += tmp
    ret = []
    ret.append(nr & AuthConf.MASK_SCRAMBLE_323)
    ret.append(nr2 & AuthConf.MASK_SCRAMBLE_323)
    return ret

def to_int8(dbl):
    num = int(floor(dbl))
    return num & 0xff

def pack_int8_array(arr):
    buf = ""
    for a in arr:
        buf += pack_int8(a)
    return buf

"""
libmysql/password.c scramble_323 117
"""
def scramble_323(scramble, passwd):
    to = []
    rand_st = RandSt()
    if len(passwd):
        hash_pwd = hash_password(passwd, len(passwd))
        hash_msg = hash_password(scramble, AuthConf.SCRAMBLE_LENGTH_323)
        randominit(rand_st, hash_pwd[0] ^ hash_msg[0], hash_pwd[1] ^ hash_msg[1])
        for i in range(AuthConf.SCRAMBLE_LENGTH_323):
            to.append(to_int8(my_rnd(rand_st) * 31 + 64))
        extra = to_int8(my_rnd(rand_st) * 31)
        for i in range(len(to)):
            to[i] ^= extra
    to.append(0)
    return pack_int8_array(to)

def client_auth_secure(scramble, passwd, idx = 0x03):
    """
    client --> server "secure" auth packet
    authentication process
    1. server --> client handshake
    2. client --> server auth
    3. server --> client auth_result
    when result of step-3 is 0xFE and CLIENT_SECURE_CONNECTION in 
    server_capabilities is on, auth password in old format
    """
    buf = scramble_323(scramble, passwd)
    header = pack_header(len(buf), idx)
    return header + buf

def client_auth_packet_body(user, pwd, db, handshake):
    """
    authentication process
    1. server --> client handshake
    2. client --> server auth
    3. server --> client auth_result
    """
    buf = ""
    buf += pack_client_flags()
    buf += pack_max_packet_size()
    buf += pack_charset_number()
    buf += pack_filln(23)
    buf += pack_string(user, True)
    buf += pack_scramble_buff(pwd, handshake)
    buf += pack_string(db, True)
    if handshake["native_password"]:
        buf += pack_string(handshake["native_password"], True)
    return buf

def sha1(seed, outputHex = False):
    if outputHex:
        return hashlib.sha1(seed).hexdigest()
    return hashlib.sha1(seed).digest()

def xor_sha1(one, two):
    buf = ""
    for i in range(len(one)):
        buf += pack_int8(unpack_int8(one[i])[0] ^ unpack_int8(two[i])[0])
    return buf

def encypt_passwd(pwd, scramble, is_native=""):
    # if is_native:
    #     return pack_int8(0x00)
    stage1_hash = sha1(pwd)
    token = xor_sha1(sha1(scramble + sha1(stage1_hash)) , stage1_hash)
    return token

def pack_scramble_buff(pwd, handshake):
    """
    generate encryption salt
    """
    scramble = encypt_passwd(pwd, handshake["scramble"], handshake["native_password"])
    scramble_len = len(scramble)
    # no password
    if scramble_len == 1:
        return scramble
    else:
        return pack_lenenc(scramble_len) + scramble

def mysqlsha1_sha1(pwd):
    if not len(pwd): return ""
    if pwd[0] == "*": pwd = pwd[1:]
    if len(pwd) % 2: return ""
    pwd = pwd.lower()
    def char2int(char):
        if char >= "0" and char <= "9":
            return ord(char) - ord("0")
        if char >= "a" and char <= "f":
            return ord(char) - ord("a") + 10
        return -1
    binstr = ""
    for i in range(len(pwd) / 2):
        num = char2int(pwd[2 * i]) << 4 | char2int(pwd[2 * i + 1])
        binstr += pack_int8(num)
    return binstr

def server_check_auth(token, scramble, pwd, isOriginal=True):
    """
    mysql server check password
    authentication process
    phase 1: server --> client handshake
    phase 2: client --> server auth
    phase 3: server --> client auth_result

    token: auth["scramble_buff"] in phase-2
    scramble: handshake["scramble"] in phase-1
    pwd: plain password or sha1(sha1(pwd))
    isOriginal: True if pwd is plain else sha1(sha1(pwd))
    """
    # utils.log(utils.cur(), pwd)
    if isOriginal:
        pwd = sha1(sha1(pwd))
    else:
        pwd = mysqlsha1_sha1(pwd)
    stage1_hash = xor_sha1(token, sha1(scramble + pwd))
    auth_pwd = sha1(stage1_hash)
    return auth_pwd == pwd

def mysqlsha1(pwd, isOriginal = True):
    """
    mysql sha1 algorithm

    pwd: plain password or sha1(sha1(pwd))
    isOriginal: plain or encrypted password
    """
    if isOriginal:
        pwd = sha1(sha1(pwd))
    return "*" + bin2hex(pwd).upper()

def char2hex(char):
    def int2char(num):
        if num < 10:
            return chr(ord("0") + num)
        else:
            return chr(ord("a") + num - 10)
    return int2char(char >> 4 & 0x0f) + int2char(char & 0x0f)

def bin2hex(binstr):
    hexstr = ""
    for i in range(len(binstr)):
        hexstr += char2hex(unpack_int8(binstr[i])[0])
    return hexstr

def client_auth_packet(user, pwd, db, handshake):
    """
    client --> server auth packet
    """
    buf = client_auth_packet_body(user, pwd, db, handshake)
    header = pack_header(len(buf))
    return header + buf

def command_packet_body(sql, cmd):
    return pack_int8(cmd) + pack_string(sql)

def command_packet(sql, cmd = 0x03):
    buf = command_packet_body(sql, cmd)
    header = pack_header(len(buf), 0)
    return header + buf

def quit_packet():
    buf = pack_int8(0x01)
    header = pack_header(len(buf), 0x00)
    return header + buf

def prepared_stmt_close_packet(statement_id=1):
    buf = pack_int8(Command.COM_STMT_CLOSE)
    buf += pack_int32(statement_id)
    header = pack_header(len(buf), 0)
    return header + buf

def unpack_error_packet(buf):
    idx = skip_header()
    field_count, idx = unpack_int8(buf, idx)
    errno, idx = unpack_int16(buf, idx)  
    sqlstate, idx = unpack_string(buf, 6, idx) # "#state"
    message, idx = unpack_string(buf, len(buf)-idx, idx)
    utils.err(utils.cur(), "%s|%s|%s|%s" % (field_count, errno, sqlstate, message))
    return (field_count, errno, sqlstate, message)

def unpack_packet_length(packet):
    return unpack_header_length(packet, 0)

def unpack_auth_packet(buf):
    """
    analyze authentication packet from client
    """
    idx = skip_header()
    auth = {
        "client_flags" : None,
        "max_packet_size" : None,
        "charset_number" : None,
        "user" : None,
        "scramble_buff" : None,
        "database" : None
    }
    try:
        auth["client_flags"], idx = unpack_int32(buf, idx)
        auth["max_packet_size"], idx = unpack_int32(buf, idx)
        auth["charset_number"], idx = unpack_int8(buf, idx)
        idx = skip_packetn(23, idx)
        auth["user"], idx = unpack_string_null(buf, idx)
        scramble_len, idx = unpack_lenenc(buf, idx)
        auth["scramble_buff"], idx = unpack_string(buf, scramble_len, idx)
        if idx < len(buf):
            auth["database"], idx = unpack_string_null(buf, idx)
    except Exception, err:
        utils.err(utils.cur(), err)
    return auth

def unpack_ok_packet(buf):
    idx = skip_header()
    val, idx = unpack_int8(buf, idx)
    if val is not AuthConf.OK_STATUS: return False
    ok = {
        "affected_rows" : None,
        "insert_id"     : None,
        "server_status" : None,
        "warning_count" : None,
        }
    try:
        ok["affected_rows"], idx = unpack_lenenc(buf, idx)
        ok["insert_id"], idx = unpack_lenenc(buf, idx)
        ok["server_status"], idx = unpack_int16(buf, idx)
        ok["warning_count"], idx = unpack_int16(buf, idx)
    except Exception, err:
        utils.err(utils.cur(), err)
    return ok

def unpack_handshake_packet(buf):
    idx = skip_header()
    handshake = {
        "protocol_version" : None,
        "server_version"   : None,
        "thread_id"        : None,
        "scramble_1"       : None,
        "server_capabilities" : None,
        "language"            : None,
        "server_status"       : None,
        "scramble_2"          : None,
        "native_password"     : None,
        "scramble"            : None
    }

    try:
        handshake["protocol_version"], idx = unpack_int8(buf, idx)
        handshake["server_version"], idx = unpack_string_null(buf, idx)
        if idx == -1:
            sys.exit(0)
        handshake["thread_id"], idx = unpack_int32(buf, idx)
        handshake["scramble_1"], idx = unpack_string(buf, 8, idx)
        idx = skip_packetn(1, idx)
        handshake["server_capabilities"], idx = unpack_int16(buf, idx)
        handshake["language"], idx = unpack_int8(buf, idx)
        handshake["server_status"], idx = unpack_int16(buf, idx)
        idx = skip_packetn(13, idx)
        if handshake["server_capabilities"] & ServerCapability.CLIENT_SECURE_CONNECTION:
            handshake["scramble_2"], idx = unpack_string(buf, 12, idx)
            idx = skip_packetn(1, idx)
        if idx < len(buf):
            handshake["native_password"], idx = unpack_string_null(buf, idx)
    except Exception, msg:
        utils.err(utils.cur(), msg)
    if handshake["scramble_2"]:
        handshake["scramble"] = handshake["scramble_1"] + handshake["scramble_2"]
    else:
        handshake["scramble"] = handshake["scramble_1"]
    return handshake

def analyze_packet(buf, is_auth = True):
    err = err_status(buf)
    if err == 0xff:
        unpack_error_packet(buf)
        return -1
    else:
        if not is_auth:
            return unpack_handshake_packet(buf)
        else:
            return unpack_auth_packet(buf)

def err_status(packet):
    idx = skip_header()
    return unpack_int8(packet, idx)[0]

def unpack_command(packet, idx = 0):
    idx = skip_header()
    tag, idx = unpack_int8(packet, idx)
    if tag == Command.COM_QUERY:
        cmd, idx = unpack_string(packet, len(packet) - idx, idx)
        utils.log(utils.cur(), cmd)
        return tag, cmd
    elif tag == Command.COM_STMT_EXECUTE:
        statement_id, idx = unpack_int32(packet, idx)
        utils.log(utils.cur(), "Statement ID", statement_id)
        return tag, statement_id
    return tag, 0

def unpack_database(packet, idx = 0):
    idx = skip_header()
    tag, idx = unpack_int8(packet, idx)
    if tag == Command.COM_INIT_DB:
        cmd, idx = unpack_string(packet, len(packet) - idx, idx)
        utils.log(utils.cur(), cmd)
        return cmd
    return tag

def is_quit_packet(packet):
    idx = skip_header()
    tag, idx = unpack_int8(packet, idx)
    return True if tag == Command.COM_QUIT else False

"""
result set package
"""
def pack_eof_packet(warning_count=0, server_status=0, idx=1):
    buf = pack_int8(0xfe)
    buf += pack_int16(warning_count)
    buf += pack_int16(server_status)
    header = pack_header(len(buf), idx)
    return header + buf

def pack_result_set_header_packet(field_count, idx=1):
    buf = pack_lenenc(field_count)
    header = pack_header(len(buf), idx)
    return header + buf

def pack_field_packet(db="", table="", org_table="", name="", \
                          org_name="", charsetnr=0x08, length=4, \
                          field_type=0xfe, flags=0x00, idx=1):
    buf = pack_string_lenenc("def")    # catalog
    buf += pack_string_lenenc(db)      # db
    buf += pack_string_lenenc(table)   # table
    buf += pack_string_lenenc(org_table) # org_table
    buf += pack_string_lenenc(name)      # name
    buf += pack_string_lenenc(org_name)  # org_name
    buf += pack_int8(0x0c)        # filler
    buf += pack_int16(charsetnr)  # charsetnr
    buf += pack_int32(length)     # length
    buf += pack_int8(field_type)  # field_type
    buf += pack_int16(0x00)  # flags
    buf += pack_int8(0x00)  # decimals
    buf += pack_int16(0x00)  # filler
    header = pack_header(len(buf), idx)
    return header + buf

def pack_row_data_packet(idx=1, *rows):
    buf = ""
    for row in rows:
        buf += pack_string_lenenc(str(row))
    header = pack_header(len(buf), idx)
    return header + buf

def pack_dict_packet(rawdata, keys=["database", "connections"], idx=1):
    if not type(rawdata) == list:
        return []
    bindata = []
    bindata.append(pack_result_set_header_packet(len(keys), idx=idx))
    idx += 1
    for keyname in keys:
        bindata.append(pack_field_packet(name=keyname, org_name=keyname, idx=idx))
        idx += 1
    bindata.append(pack_eof_packet(idx=idx))
    idx += 1
    for data in rawdata:
        if len(data) != len(keys): continue
        bindata.append(pack_row_data_packet(idx, *data))
        idx += 1
    bindata.append(pack_eof_packet(idx=idx))
    return bindata

