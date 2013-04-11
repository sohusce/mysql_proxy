#!/usr/bin/env python
#-*- coding: utf-8 -*-
######################################################################
## Filename:      utils.py
##
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Mon Feb 13 10:28:36 2012
##
## Description:
##
######################################################################
import urllib, urllib2, getopt, binascii, inspect, __builtin__
import re, sys, syslog, logging, logging.handlers
import cjson, time, pwd, hashlib, collections
import socket, fcntl, struct, threading, hmac
from collections import OrderedDict

import constants, aes_helper, mc_helper
from constants import ProxyServer, HTTPServer, Cipher, Version, DBAAPI

logger = errlog = None
aesins = aes_helper.AESModeOfOperation()

def hmac_md5(query, key=DBAAPI.SECRET):
    if not isinstance(query, dict): return ""
    sorted_query = OrderedDict(sorted(query.items(), key=lambda t: t[0]))
    url_query = "&".join(["%s=%s" % (k, v) for k, v in sorted_query.iteritems() if k is not "magic"])
    return hmac.new(key, "".join([url_query, DBAAPI.SECRET])).hexdigest()

def gen_cipher_key(key, size):
    assert size in (Cipher.SIZE_128, Cipher.SIZE_192, Cipher.SIZE_256)
    l = [ord(x) for x in key[:size]]
    x = size - len(l)
    return (l + [0] * x) if x > 0 else l

def encrypt(plaintext, key=Cipher.SALT, mode=Cipher.OFB, size=Cipher.SIZE_128, iv=Cipher.IV):
    salt = gen_cipher_key(key, size)
    mod, orig_len, ciph = aesins.encrypt(plaintext, mode, salt, size, iv)
    return binascii.hexlify("".join([chr(x) for x in ciph])).upper()

def decrypt(ciphertext, key=Cipher.SALT, mode=Cipher.OFB, size=Cipher.SIZE_128, iv=Cipher.IV):
    try:
        l = [ord(x) for x in binascii.unhexlify(ciphertext)]
    except:
        return ''
    salt = gen_cipher_key(key, size)
    return aesins.decrypt(l, len(ciphertext), mode, salt, size, iv)

def getuidgid():
    try:
        info = pwd.getpwnam(ProxyServer.APPNAME)
        return info.pw_uid, info.pw_gid
    except:
        return 0, 0

def gen_logger(logger_name, filename, errfile=None):
    global logger, errlog
    def one_logger(logger_name, filename):
        formatter = logging.Formatter("%(asctime)s|%(levelname)s|SCEDB-%(servertype)s|%(serverip)s|%(clientip)s|%(dbname)s|%(message)s")
        # hdlr = logging.FileHandler(filename)
        # if filename.endswith(".log"):
        #     filename = ".".join(filename.split(".")[:-1])
        hdlr = logging.handlers.TimedRotatingFileHandler(filename, "midnight", 1)
        hdlr.suffix = "%Y%m%d.log"
        hdlr.setFormatter(formatter)
        o = logging.getLogger(logger_name)
        o.addHandler(hdlr)
        o.setLevel(logging.DEBUG)
        return o
    if not errfile:
        l = filename.split('.', 1)
        errfile = l[0] + '_error.' + l[1]
    logger = one_logger(logger_name, filename)
    errlog = one_logger(logger_name + '_error', errfile)

logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s|%(levelname)s|SCEDB-%(servertype)s|%(serverip)s|%(clientip)s|%(dbname)s|%(message)s"
    )

def getip(ifname="eth0"):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack("256s", ifname[:15])
            )[20:24])

def change_logconf(srv):
    assert isinstance(srv, dict)
    global logconf
    for k, v in srv.items():
        if k in logconf:
            logconf[k] = v

def reset_logconf():
    global logconf
    logconf={
        "servertype": "PROXY",
        "serverip": getip(),
        "clientip": "-",
        "dbname": "-",
        }
reset_logconf()

def cur():
    f = inspect.currentframe().f_back
    return ':'.join([f.f_code.co_filename.rsplit('/', 1)[-1], f.f_code.co_name, str(f.f_lineno)])

def log(*msg):
    x = ';'.join([str(x) for x in msg])
    try:
        logger.info(x, extra=logconf)
    except Exception, err:
        pass

def err(*msg):
    x = ';'.join([str(x) for x in msg])
    try:
        errlog.error(x, extra=logconf)
    except Exception, err:
        pass

def md5(*args):
    m = hashlib.md5()
    for arg in args:
        m.update(str(arg))
    return m.hexdigest()

def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")

class TooManyConnections(Exception):
    def __init__(self, dbname, maxconns):
        self.message = "Database '%s' has exceeded %s connections" % (dbname, maxconns)

    def __str__(self):
        return self.message

class NoConnections(Exception):
    def __init__(self, dbname):
        self.message = "Database '%s' has no connections, build a new connection" % (dbname,)

    def __str__(self):
        return self.message

class StatsConns(object):
    def __init__(self):
        self.conns = collections.defaultdict(int)
        self._mincached = ProxyServer.POOL_MIN
        self._maxcached = ProxyServer.POOL_MAX
        self.rlock = threading.Lock()
        self.mc = mc_helper.Memcached()

    def mc_incr(self, db, delta=1):
        try:
            result = self.mc.incr(db, delta)
            if result is None:
                self.mc.set(db, 0)
                return self.mc.incr(db, delta)
            return result
        except Exception, err:
            self.mc.set(db, 0)
            return self.mc.incr(db, delta)

    def mc_decr(self, db, delta=1):
        try:
            result = self.mc.decr(db, delta)
            if result is None:
                self.mc.set(db, 0)
                return self.mc.decr(db, delta)
            return result
        except Exception, err:
            self.mc.set(db, 0)
            return self.mc.decr(db, delta)

    def mc_get(self, db):
        try:
            result = self.mc.get(db)
            if result is None:
                self.mc.set(db, 0)
                return self.mc.get(db)
            return result
        except Exception, err:
            self.mc.set(db, 0)
            return self.mc.get(db)

    def incr(self, db, delta=1):
        if not db: return
        with self.rlock:
            self.conns[db] += delta
            self.mc_incr(db, delta)

    def check(self, db):
        if self.mc_get(db) >= self._maxcached:
            raise TooManyConnections(db, self.mc_get(db))

    def decr(self, db, delta=1):
        if not db: return
        with self.rlock:
            if self.conns[db] > 0:
                self.conns[db] -= delta
                self.mc_decr(db, delta)

    def get(self, db):
        if not db: return 0
        with self.rlock:
            return self.conns[db]

    def show(self):
        return self.conns.items()

    def pool(self):
        conns = []
        for db in self.conns:
            conns.append((db, self.mc_get(db)))
        return conns

    def close(self):
        self.mc.close()

    def __str__(self):
        return cjson.encode(self.conns)

__builtin__.StatsConns = StatsConns

"""
mysqlsha1 algorithm
"""
def sha1(seed, outputHex=False):
    o = hashlib.sha1(seed)
    return o.hexdigest() if outputHex else o.digest()

def mysqlsha1(pwd, isOriginal=True):
    """
    mysql sha1 algorithm
    """
    return '*' + binascii.hexlify(sha1(sha1(pwd)) if isOriginal else pwd).upper()

class Request(object):
    def __init__(self, url, param = {}, headers = {}):
        if param:
            data = urllib.urlencode(param)
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            headers["Content-Length"] = len(data)
        else:
            data = None

        log(cur(), "%s%s %s" % ("="*10, ">", url))
        log(cur(), param)
        log(cur(), data)
        log(cur(), headers)
        self.request = urllib2.Request(url, headers=headers)
        self.response = urllib2.urlopen(self.request, data)

    def read(self):
        return self.response.read()

def response_deco(func):
    def call(start_response, response, header=None, forceOK=False):
        code = int(func.func_name.split("_")[1])
        return Response.gen_response(start_response, code, Response.HTTP_STATUS_CODE[code], response, header, forceOK)
    return call

class Response(object):

    HTTP_STATUS_CODE = {
        200 : "OK",
        302 : "Found",
        304 : "Not Modified",
        401 : "Unauthorized",
        403 : "Forbidden",
        404 : "Not Found",
        405 : "Method Not Allowed",
        500 : "Internal Server Error",
        501 : "Not Implemented"
        }

    DEFAULT_RESPONSE_HEADER = [("Content-Type", "application/json"), ("Server", "Sohu SCE-DB Server")]

    @staticmethod
    @response_deco
    def make_200(Response, start_response, response, header=None, forceOK=False):
        pass

    @staticmethod
    @response_deco
    def make_401(Response, start_response, response, header=None, forceOK=False):
        pass

    @staticmethod
    @response_deco
    def make_404(Response, start_response, response, header=None, forceOK=False):
        pass

    @staticmethod
    @response_deco
    def make_500(Response, start_response, response, header=None, forceOK=False):
        pass

    @staticmethod
    def gen_response(start_response, httpcode, codestr, response, header=None, forceOK=False):
        headers = list(Response.DEFAULT_RESPONSE_HEADER)
        if header and isinstance(header, list):
            headers.extend(header)

        if isinstance(response, dict):
            if set(response) == set(HTTPServer.RESPONSE):
                tmpdict = dict(response)
            else:
                tmpdict = dict(HTTPServer.RESPONSE)
                httpcode = 200
                tmpdict["result"] = HTTPServer.RESPONSE_SUCCESS
                tmpdict["context"] = response
        else:
            if forceOK: httpcode = 200
            tmpdict = dict(HTTPServer.RESPONSE)
            tmpdict["result"] = HTTPServer.RESPONSE_SUCCESS if forceOK else HTTPServer.RESPONSE_FAILURE
            tmpdict["context"] = response

        tmpdict["code"] = httpcode
        tmpdict["msg"] = Response.HTTP_STATUS_CODE[httpcode]
        start_response("%s %s" % (httpcode, Response.HTTP_STATUS_CODE[httpcode]), headers)
        return cjson.encode(tmpdict)

def parse_args(args):
    versions = [x.lower() for x in Version.VERSIONS]
    l = [x.split('=', 1)[1] for x in args if x.startswith('--prefix=')]
    if l and l[0] in versions:
        constants.VERSION = Version.VERSIONS[l[0].upper()]
        constants.change_config()
        return
    err(cur(), 'Command line parameter must contain --prefix=value and value must be one of %s, but not %r' % ('/'.join(versions), args))
    sys.exit(0)

def nc(address):
    if not isinstance(address, tuple): return True
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.1)
        sock.connect(address)
        sock.close()
        return False
    except Exception, err:
        log(cur(), err, address)
    return True

