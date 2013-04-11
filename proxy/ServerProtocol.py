#!/usr/bin/env python
#-*- coding:utf-8 -*-
######################################################################
## Filename:      ServerProtocol.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Thu Feb  2 15:17:20 2012
##                
## Description:   ServerProtocol
##                
######################################################################
import sys, cjson, time, os.path, traceback
from twisted.internet import protocol, reactor

import threads, sql_parser
from helper import utils, ip_helper
from helper.constants import *
from mysql_packet import *

def change_log(func):
    def change(obj, *args, **kwargs):
        srv = {
            "clientip": obj.transport.getPeer().host,
            "dbname": obj.database or "-"
            }
        utils.change_logconf(srv)
        return func(obj, *args, **kwargs)
    return change

def reset_log(func):
    def reset(obj, *args, **kwargs):
        utils.reset_logconf()
        return func(obj, *args, **kwargs)
    return reset

def set_idx(seq=0):
    def _call(func):
        def _set_idx(*args, **kwargs):
            try:
                pktdata = args[seq]
                args[0].idx = ord(pktdata[3])
            except:
                pass
            return func(*args, **kwargs)
        return _set_idx
    return _call

class ServerProtocol(protocol.Protocol):

    def __init__(self):
        self.client_authed = False
        self.server = None
        
        self.database = None
        self.dbname = None
        # self.node_listen = True

        # SQL parser
        self.sql_parser = sql_parser.SQLVerify()

        # force read/write
        self.rwsplit = True

        self.timeout = threads.WaitTimeout(self.close_proobj, self)
        self.timeout.start()

        # ip whitelist
        self.ips = None

        self.idx = -1
        self.dbtype = ""

        # prepared statement id
        self.stmt_id = 0

        # requests
        self.requests = 0
        # first.request
        self.req_1st = True
        # receiving trunk
        self.in_trunk = False
        self.buffer = ""
        self.pack_len = 0

    def reset_requests(self):
        self.requests = 0

    @property
    def next_idx(self):
        self.idx += 1
        self.idx %= 256
        return self.idx
    
    def close_proobj(self):
        # utils.log(utils.cur(), "close_proobj")
        self.transport.loseConnection()

    @reset_log
    def connectionLost(self, reason):
        utils.reset_logconf()
        self.factory.conns -= 1
        utils.log(utils.cur(), "client is losing proxy %s" % self.factory.conns, self.requests)
        # mysql_stmt_close
        if self.stmt_id:
            self.server.mysql_stmt_close(self.stmt_id)
        # reclaim connection
        self.factory.takeServer(self.server, self)
        # clean connection
        self.server = None
        self.timeout.stop()

    @change_log
    def connectionMade(self):
        self._write(self.factory.getHandshakeRaw())

    def _checkForbid(self, opts, dbtype):
        if dbtype == ZKConf.INTERNAL or (self.database not in self.factory.forbidinfo):
            return False

        db  = self.database
        msg = self.factory.forbidinfo[db]

        if msg["type"] == Forbid.FORBID_FOREVER:
            return self.sendForbid(msg, db, opts)
        elif msg["type"] == Forbid.FORBID_WORKING:
            start, duration = msg["start"], msg["duration"]
            if start + duration > time.time():
                return self.sendForbid(msg, db, opts)

        self.factory.zk.erase_forbid(db)
        return False

    def sendConnectionTimeout(self):
        timeout_error = dict(ErrorCode.BACKEND_TIMEOUT)
        self._write(self._goWrong(timeout_error, self.next_idx))
        return True

    def sendForbid(self, data, db, opts):
        """
        check contraints of databases, tables and operations
        """
        assert isinstance(data, dict)
        assert "object" in data and data["object"] in (Forbid.FORBID_DATABASE, Forbid.FORBID_TABLE)

        def _realForbid(crud, dbtb, errmsg, isDB=True):
            forbid_duration = int(data["start"] + data["duration"] - time.time()) if data["type"] == Forbid.FORBID_WORKING else sys.maxint
            forbid_error = dict(ErrorCode.QUOTA_EXCEEDED)
            forbid_error["message"] = forbid_error["message"] % (crud, "Database" if isDB else "Table", dbtb, errmsg, forbid_duration)
            self._write(self._goWrong(forbid_error, self.next_idx))
            return True

        crud = data["crud"]
        utils.log(utils.cur(), crud)
        if not crud: return False

        if data["object"] == Forbid.FORBID_DATABASE:
            allopts = opts["db"]
            utils.log(utils.cur(), allopts)
            assert type(allopts) is list
            if Forbid.OPERATION_DEFAULT in crud:
                return _realForbid([Forbid.OPERATION_DEFAULT], db, data["errmsg"], True)
            else:
                if set(allopts).intersection(set(crud)):
                    return _realForbid(crud, db, data["errmsg"], True)
        else:
            allopts = opts["tb"]
            utils.log(utils.cur(), allopts)
            assert type(allopts) is dict            
            for tbl, tbopts in allopts.iteritems():
                if tbl in crud:
                    if Forbid.OPERATION_DEFAULT in crud[tbl]:
                        return _realForbid([Forbid.OPERATION_DEFAULT], tbl, data["errmsg"], False)
                    if set(tbopts).intersection(set(crud[tbl])):
                        return _realForbid(crud[tbl], tbl, data["errmsg"], False)                    

        return False

    @set_idx(1)
    @change_log
    def dataReceived(self, data):
        # client auth first
        if not self.client_authed:
            return self._clientAuth(data)

        # normal sql request, if quit packet return
        if not data or len(data) == 0 or is_quit_packet(data):
            return

        def _r_(request):
            self.requests += 1
            if self.req_1st:
                reactor.callLater(ProxyServer.CONN_TIMEOUT, self.checkFirstReq)
            self._dealQuery(request)

        if self.in_trunk:
            self.buffer += data
            if len(self.buffer) == self.pack_len + 4:
                self.in_trunk = False
                _r_(self.buffer)
        else:
            self.pack_len, _ = unpack_packet_length(data)
            if self.pack_len + 4 > len(data):
                self.in_trunk = True
                self.buffer = data
            elif self.pack_len + 4 == len(data):
                _r_(data)
                        
    def checkFirstReq(self):
        if self.req_1st:
            self.server.close_rw()
            self.sendConnectionTimeout()

    def _clientAuth(self, data):
        """
        1. parse auth packet, db is mandatory
        """
        auth = analyze_packet(data)
        if auth == -1 or not auth["database"]:
            self._write(self._goWrong(ErrorCode.PACKET_WRONG, self.next_idx))
            return

        """
        2. retrieve database info from zookeeper and auth
        """
        is_legal = self._zookeeperAuth(auth)
        utils.log(utils.cur(), "after zkauth", is_legal)
        """
        3. assign a pair of read/write connections when passed
        """
        if is_legal:
            self.database = auth["database"]
            self.dbname = self.getDBname()
            utils.log(utils.cur(), self.database)

            def callback(me, old=False):
                # utils.log(utils.cur(), "auth inside")
                pkts = me.server.writeClient.protocol.packets
                if old:
                    me._write(me._goRight(me.next_idx))
                    me.client_authed = True
                    stats_conns.incr(self.database)
                    utils.log(utils.cur(), stats_conns)
                    return

                if pkts["err"]:
                    me._write(pkts["err"])
                else:
                    """
                    ** NOTICE **
                    be care of index in pkts["ok"] and pkts["err"]
                    """
                    # utils.log(utils.cur(), pkts)
                    p = pkts["ok"][:3] + "\x02" + pkts["ok"][4:]
                    me._write(p)
                    me.client_authed = True
                    stats_conns.incr(self.database)
                    utils.log(utils.cur(), stats_conns)
            
            self.factory.getServer(self.database, callback, self)

    def _goWrong(self, errstate, idx = 1):
        return pack_err_packet(errstate["errno"], errstate["sqlstate"], errstate["message"], idx)

    def pool_error(self, err):
        error = dict(ErrorCode.POOL_ERROR)
        error["message"] = error["message"] % str(err)
        self._write(self._goWrong(error, self.next_idx))

    def service_error(self, host):
        error = dict(ErrorCode.SERVICE_ERROR)
        error["message"] = error["message"] % str(host)
        self._write(self._goWrong(error, self.next_idx))

    def _goRight(self, idx = 1):
        return pack_ok_packet(idx=idx)

    def _zookeeperAuth(self, auth):
        is_legal = True
        """
        1. check whether client IP is authorized
        """
        try:
            ippath = os.path.join(ZKConf.ZK_PATH_DB, auth["database"], ZKConf.KEY_IP)
            # utils.log(utils.cur(), ippath)
            is_legal = self.factory.get_path(ippath)
            # utils.log(utils.cur(), is_legal)
            if is_legal is not False:
                ip_json = is_legal
                is_legal = True
                # utils.log(utils.cur(), ip_json)
                try:
                    self.ips = ip_helper.IpRangeList(*tuple(cjson.decode(ip_json)))
                except:
                    self.ips = None
                peer = self.transport.getPeer()
                utils.log(utils.cur(), peer, self.ips, self.factory.ips)
                if not ((self.ips and peer.host in self.ips) or 
                        (self.factory.ips and peer.host in self.factory.ips)):
                    is_legal = False
                    ip_error = dict(ErrorCode.IP_RESTRICTED)
                    ip_error["message"] = ip_error["message"] % {"ip":peer.host}
                    self._write(self._goWrong(ip_error, self.next_idx)) # 2
                    return is_legal
        except Exception, err:
            utils.err(utils.cur(), traceback.format_exc())
            is_legal = False

        # utils.log(utils.cur(), "ip disabled", is_legal)

        """
        1.1 check whether DB is forbidden
        """
        try:
            if is_legal:
                dis_path = os.path.join(ZKConf.ZK_PATH_DB, auth["database"], ZKConf.KEY_DISABLED)
                is_legal = self.factory.get_path(dis_path)
                if is_legal is not False:
                    disabled = is_legal
                    is_legal = True
                    if int(disabled) == ZKConf.DISABLED:
                        is_legal = False
                        db_error = dict(ErrorCode.DB_DISABLED)
                        db_error["message"] = db_error["message"] % {"db":auth["database"]}
                        self._write(self._goWrong(db_error, self.next_idx))
                        return is_legal
        except Exception, err:
            utils.err(utils.cur(), err)
            is_legal = False

        # utils.log(utils.cur(), "db disabled", is_legal)

        try:
            if is_legal:
                dbtype_path = os.path.join(ZKConf.ZK_PATH_DB, auth["database"], ZKConf.KEY_DBTYPE)
                # utils.log(utils.cur(), dbtype_path)
                is_legal = self.factory.get_path(dbtype_path)
                if is_legal is not False:
                    self.dbtype = is_legal
                    is_legal = True
        except Exception, err:
            utils.err(utils.cur(), traceback.format_exc())
            is_legal = False

        # utils.log(utils.cur(), "db forbidden", is_legal)
        """
        2. find /DB_INFO/database node and retrieve DB_PASS, DB_USER, DB_DB
        """
        try:
            if is_legal:
                dbpath = os.path.join(ZKConf.ZK_PATH_DB, auth["database"], ZKConf.KEY_DBCONF)
                is_legal = self.factory.get_path(dbpath)
                if is_legal is not False:
                    is_legal = True
                    info = {"passwd": "", "user": "", "database":""}
                    info["passwd"]   = self.factory.get_path(dbpath + "/" + ZKConf.KEY_PASSWORD)
                    info["user"]     = self.factory.get_path(dbpath + "/" + ZKConf.KEY_USER)
                    info["database"] = auth["database"]
                    info["passwd"] = utils.mysqlsha1(utils.decrypt(info["passwd"]))
        except Exception, err:
            utils.err(utils.cur(), err)
            is_legal = False

        # utils.log(utils.cur(), "path existed", is_legal)
        """
        3. check whether there is password
        """
        if is_legal and not auth["scramble_buff"]: is_legal = False
        # utils.log(utils.cur(), "passwd has", is_legal)
        """
        4. check whether password is correct
        """
        if is_legal:
            for k in info:
                if k == "passwd" and server_check_auth(auth["scramble_buff"], self.factory.getHandshakeDic()["scramble"], info[k], False): 
                    # utils.log(utils.cur(), "passwd auth success")
                    continue
                elif k in auth and info[k] == auth[k]: 
                    continue
                else:
                    utils.log(utils.cur(), "auth failed", k)
                    is_legal = False
                    break

        # utils.log(utils.cur(), "passwd right", is_legal)

        """
        5. return error message of authentication
        """
        if not is_legal:
            utils.log(utils.cur(), "illegal")
            auth_error = dict(ErrorCode.AUTH_WRONG)
            auth["host"] = self.transport.getHost().host
            auth_error["message"] = auth_error["message"] % (auth["user"], auth["host"], "YES" if auth["scramble_buff"] else "NO")
            self._write(self._goWrong(auth_error, self.next_idx)) # 2
            return is_legal

        return is_legal

    def getDBname(self):
        # rewrite "USE $self.database" to "USE dbname"
        dbname = None
        try:
            dbpath = os.path.join(ZKConf.ZK_PATH_DB, self.database, ZKConf.KEY_DBCONF, ZKConf.KEY_DB)
            dbname = self.factory.get_path(dbpath)
            utils.log(utils.cur(), dbpath, dbname)
        except:
            pass
        return dbname

    def _dealQuery(self, data):
        tag, cmd = unpack_command(data)
        """
        if cmd is not COM_QUERY then forward data directly
        """
        if isinstance(cmd, int): 
            if tag == Command.COM_INIT_DB:
                token_db = unpack_database(data)
                utils.log(utils.cur(), token_db)
                dbname = self.dbname

                if (not dbname) or (token_db not in (dbname, self.database)):
                    self.timeout.reset()
                    self._write(self._goWrong(ErrorCode.USE_FORBIDDEN, self.next_idx))
                    return

                if token_db == self.database and dbname != self.database:
                    data = command_packet(dbname, Command.COM_INIT_DB)
            elif tag == Command.COM_STMT_EXECUTE:
                self.stmt_id = cmd

            self.timeout.reset()
            if self.server:
                self.server.writeClient.protocol.queryRaw(data)
            return

        forceOk = False
        tokens = self.sql_parser.addSQL(cmd)
        dbname = self.dbname

        if len(tokens) and tokens[0][0] == "USE":
            if (not dbname) or (len(tokens) != 2) or (tokens[1][0] not in (dbname.upper(), self.database.upper())):
                self.timeout.reset()
                self._write(self._goWrong(ErrorCode.USE_FORBIDDEN, self.next_idx))
                return
            
            forceOk = True
            if dbname != self.database and tokens[1][0] == self.database.upper():
                data = command_packet("USE `%s`" % dbname)

        use_master, sql_state, msg, opts = self.sql_parser.verify(self.dbtype, forceOk, self.factory.servers, self.factory.busy_proobj, self.idx+1)
        utils.log(utils.cur(), use_master, sql_state, msg, opts)

        if not isinstance(use_master, int):
            self.timeout.reset()
            if use_master == "err":
                self._write(self._goWrong(msg if isinstance(msg, dict) else ErrorCode.SQL_FORBIDDEN, self.next_idx))
                return
            elif use_master == "ok":
                if sql_state == SQLState.SQL_PRIVATE:
                    if type(msg) == list:
                        """
                        sce pool/proxy/client status
                        """
                        for result_set_cell in msg:
                            self._write(result_set_cell)
                        return
                    else:
                        """
                        sce master = 0/1 whether read/write is splitting
                        """
                        self.rwsplit = False if msg else True
                        utils.log(utils.cur(), msg, type(msg), self.rwsplit, self.idx)
                        self._write(self._goRight(self.next_idx))
                        return

        if self._checkForbid(opts, self.dbtype): return

        """
        read/write splitting
        """
        if self.server:
            in_trans = self.server.writeClient.protocol.in_trans
            # non-transactional SELECT
            if self.rwsplit and in_trans == False and use_master == False:
                # read
                utils.log(utils.cur(), "read")
                self.timeout.reset()
                self.server.readClient.protocol.queryRaw(data)
            else:
                # write
                utils.log(utils.cur(), "write")
                self.timeout.reset()
                self.server.writeClient.protocol.queryRaw(data)

    def _write(self, data):
        if self.req_1st:
            self.req_1st = False
        self.transport.write(data)
