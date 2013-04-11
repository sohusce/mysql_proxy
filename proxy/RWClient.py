#!/usr/bin/env python
# -*- coding: utf-8 -*-
######################################################################
## Filename:      RWClient.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Fri Mar  2 15:35:49 2012
##                
## Description:   
##                
######################################################################
import os, time, threading
from collections import namedtuple
from twisted.internet import tcp, protocol, reactor

import threads, traceback
from mysql_packet import *
from helper import zk_helper, utils
from helper.constants import MyState, AuthConf, ServerCapability, ZKConf, ProxyServer

class DBClientProtocol(protocol.Protocol):
    
    def __init__(self, user, passwd, db, obj, name, dbshow):
        self.is_authed = False
        self.secure_authed = False
        self.status = MyState.MY_UNKNOWN
        self.packets = {"handshake_raw": None, "handshake_dic": None, "auth": None, "ok": None, "err": None, "quit": None}
        self.user = user
        self.passwd = passwd
        self.db = db
        self.dbshow = dbshow
        self.name = name
        self.in_trans = False

        # thread for writing database
        self.stats_thread = threads.ProxyStats("stats_thread", self.dbshow)
        self.stats_thread.start()

    def log(self, *args):
        if self.name:
            utils.log(self.name, *args)
        else:
            utils.log(*args)

    def err(self, *args):
        if self.name:
            utils.err(self.name, *args)
        else:
            utils.err(*args)

    def connectionMade(self):
        self._setStatus(MyState.MY_INIT)

    def _dealHandshake(self, data):
        # handshake server --> proxy
        handshake = analyze_packet(data, self.is_authed)
        if handshake == -1:
            raise Exception("MyProxy couldn't connect server")
        self._setPacket(handshake, "handshake_dic")
        self._setPacket(data, "handshake_raw")
        # auth proxy --> server
        auth = client_auth_packet(self.user, self.passwd, self.db, self.getHandshakeDic())
        self._setPacket(auth, "auth")
        self._write(auth)
        self._setStatus(MyState.MY_SEND_AUTH)

    def _dealAuthResult(self, data):
        # ok/ err packet
        err = err_status(data)
        self.log(utils.cur(), "ok/err", "%02X" % err, len(data))
        pkts = self._getPacket("handshake_dic")
        if err == AuthConf.OK_STATUS:
            self.is_authed = True
            self._setStatus(MyState.MY_AUTHED)
            self._setPacket(data, "ok")
            self._setObjPackets(self.factory.obj) # set packets
            self._invokeRWClient(self.factory.rwobj)# invoke callback
        elif err == AuthConf.EOF_STATUS and len(data) == 5 and pkts["server_capabilities"] & ServerCapability.CLIENT_SECURE_CONNECTION:
            """
            @date Thu Nov 24 19:30:10 CST 2011
            @from sql-common/client.c CLI_MYSQL_REAL_CONNECT() 2414
            @desc By sending this very specific reply server asks us to send scrambled password in old format.
            """
            # already authenticated once before
            if self.secure_authed:
                self._setStatus(MyState.MY_UNAUTHED)
                self._setPacket(data, "err")
                raise Exception("cannot secure authed twice")

            auth_secure = client_auth_secure(pkts["scramble"], self.passwd)
            self._write(auth_secure)
            self._setStatus(MyState.MY_SEND_AUTH_SECURE)
            self.secure_authed = True
        else:
            self._setStatus(MyState.MY_UNAUTHED)
            self._setPacket(data, "err")
            self.log(utils.cur(), unpack_error_packet(data))
            # send data back to protocolObj
            self._closeFrontend(self.factory.obj, data)

    def _setObjPackets(self, obj):
        if obj and hasattr(obj, "packets") and obj.packets == None and self.packets["auth"]:
            obj.packets = self.packets

    def _invokeRWClient(self, rwclient):
        if hasattr(rwclient, "invoke"):
            rwclient.invoke()

    def _sendBack(self, obj, data):
        # self.log(utils.cur(), "ready to sendback", id(obj), obj)
        try:
            if self.name:
                for name, rawsql in self.factory.rwobj.raw_sql:
                    if name == self.name:
                        self.factory.rwobj.raw_sql.remove((name, rawsql))
                        break
        except:
            pass
        if hasattr(obj, "_write"):
            ok_pack = unpack_ok_packet(data)
            if ok_pack and \
                    "server_status" in ok_pack and \
                    ok_pack["server_status"] and \
                    (ok_pack["server_status"] & AuthConf.SERVER_STATUS_IN_TRANS):
                self.in_trans = True
            else:
                self.in_trans = False
            obj._write(data)

    def _closeFrontend(self, obj, data):
        self.log(utils.cur(), "ready to close frontend", obj)
        if hasattr(obj, "_write"):
            self.log(utils.cur(), "writing", obj.idx, obj, self.factory.rwobj)
            # close connection between client and proxy
            data = data[:3] + pack_int8(obj.idx+1) + data[4:]
            # quit when connection failure, or retry connect
            self.factory.quitting = True
            obj._write(data)
            def later():
                obj.close_proobj()
                # close connection between proxy and mysql-server (RWConnection)
                self.factory.rwobj.quit_all()
            reactor.callLater(5, later)

    def dataReceived(self, data):
        status = self._getStatus()
        # receive handshake, send auth
        if status == MyState.MY_INIT:
            try:
                self._setStatus(MyState.MY_RECV_HAND)
                self._dealHandshake(data)
            except Exception, msg:
                self.err(utils.cur(), msg)
                # send back data to protocolObj
                self._closeFrontend(self.factory.obj, data)
        elif status == MyState.MY_RECV_HAND:
            pass
        # receive auth_result
        elif status == MyState.MY_SEND_AUTH:
            try:
                self._setStatus(MyState.MY_RECV_AUTH_RESULT)
                self._dealAuthResult(data)
            except Exception, msg:
                self.err(utils.cur(), traceback.format_exc())
        # receive secure_auth_result
        elif status == MyState.MY_SEND_AUTH_SECURE:
            try:
                self._setStatus(MyState.MY_RECV_AUTH_SECURE_RESULT)
                self._dealAuthResult(data)
            except Exception, msg:
                self.err(utils.cur(), traceback.format_exc())
        elif status == MyState.MY_RECV_AUTH_RESULT:
            pass
        # receive query_result, send back to client
        elif status == MyState.MY_AUTHED:
            ret = analyze_packet(data)
            # traffic statistics, both traffic-in and traffic-out
            self.stats_thread.grow(len(data), True)
            # send back data to protocolObj
            self._sendBack(self.factory.obj, data)
        elif status == MyState.MY_UNAUTHED:
            pass
        else:
            raise Exception("Impossible")

    def query(self, sql):
        data = command_packet(sql)
        self.log(utils.cur(), self.is_authed)
        if self.is_authed:
            if self.name:
                self.factory.rwobj.raw_sql.append((self.name, data))
            self.stats_thread.grow(len(data), False)
            self._write(data)

    def queryRaw(self, data):
        # self.log(utils.cur(), self.is_authed, id(self.factory.rwobj))
        if self.is_authed:
            if self.name:
                self.factory.rwobj.raw_sql.append((self.name, data))
            self.stats_thread.grow(len(data), False)
            self._write(data)

    def connectionLost(self, reason):
        self.log(utils.cur(), reason.getErrorMessage())
        """
        ** WARNINGS **
        may block in MySQL read/write process, then block twisted reactor thread
        """
        self.stats_thread.write()
        self.stats_thread.stop()

    def getDb(self):
        return self.db
    
    def getHandshakeRaw(self):
        return self.packets["handshake_raw"]

    def getHandshakeDic(self):
        return self.packets["handshake_dic"]

    def goWrong(self, errstate):
        return pack_err_packet(errstate["errno"], 
                               errstate["sqlstate"], 
                               errstate["message"])

    def closePreparedStmt(self, stmt_id):
        packet = prepared_stmt_close_packet(stmt_id)
        try:
            self.transport.writeSomeData(packet)
            self.log(utils.cur(), "mysql_stmt_close done")
        except Exception, err:
            self.err(utils.cur(), err)

    def sendQuit(self):
        if not self.is_authed: return
        packet = quit_packet()
        """
        Question: Why using low-level socket operations, which are
                  self.tranport.socket.send(data) or
                  self.transport.writeSomeData(data),
                  instead of reliable socket wrapper in Twisted, which is
                  self.transport.write(data) ?
        Answer: The latter is monitored by Twisted reactor, which underlying
                network I/O models are select, poll or epoll. "The data is 
                buffered until the underlying file descriptor is ready 
                for writing", class addWriter send data when polled by reactor,
                which is asynchronous and causes lantency.
                The former send data directly.
        """
        # self.transport.socket.send(packet)
        try:
            self.transport.writeSomeData(packet)
            self.log(utils.cur(), "write done")
        except Exception, err:
            self.err(utils.cur(), err)

    def _write(self, data):
        self.transport.write(data)

    def _getStatus(self):
        return self.status

    def _setStatus(self, status):
        self.status = status

    def _setPacket(self, data, key):
        if key in self.packets:
            self.packets[key] = data

    def _getPacket(self, key):
        if key in self.packets:
            return self.packets[key]
        else:
            return None

class DBClientFactory(protocol.ClientFactory):

    def __init__(self, dbinfo, obj, rwobj=None, name=""):
        self.tries = 5;
        self.dbinfo = dbinfo
        self.quitting = False
        self.rwobj = rwobj
        self.obj = obj
        self.name = name

    def log(self, *args):
        if self.name:
            utils.log(self.name, *args)
        else:
            utils.log(*args)

    def err(self, *args):
        if self.name:
            utils.err(self.name, *args)
        else:
            utils.err(*args)
        
    def startedConnecting(self, connector):
        pass

    def closeStmt(self, stmt_id):
        try:
            self.protocol.closePreparedStmt(stmt_id)
        except Exception, err:
            self.err(utils.cur(), err)

    def quit(self):
        try:
            self.quitting = True
            self.protocol.sendQuit()
        except Exception, err:
            self.err(utils.cur(), err)

    def clientConnectionFailed(self, connector, reason):
        self.log(utils.cur(), reason.getErrorMessage())
        if hasattr(self.rwobj, "decr"): self.rwobj.decr()
        if self.rwobj:
            err_type = "host_r" if self.name == "--read--" else "host_w"
            self.rwobj.deal_conn_err(self.dbinfo[err_type])

    def clientConnectionLost(self, connector, reason):
        """
        @date Tue Nov 29 18:02:46 CST 2011
        Question: Why should we do like this as follows?
        Answer:
          1. DBClientFactory contains connection between proxy and mysql-server
          2. In general, mysql-server has "wait_timeout" system variable,
             "MySQL server has gone away" error occurs 
             when client is noninteractive for at leat wait_timeout seconds.
          3. So, re-connect in function **clientConnectionLost**.
          4. And if DBClientFactory close connection, **clientConnectionLost**
             is also invoked.
          5. When condition-4 happens, re-connection will result in creation of
             new DBClientFactory instance, while old DBClientFactory instance is
             still exists.
          6. How to solve problems of condition-5? **quitting** flag indicates
             whether there is a request for disconnecting. If **quitting** flag
             is on, then clientConnectionLost do nothing, otherwise disconnect.
        """
        self.log(utils.cur(), self.quitting, reason.getErrorMessage())
        if hasattr(self.rwobj, "decr"): self.rwobj.decr()
        if not self.quitting:
            connector.connect()

    def setObj(self, obj):
        self.obj = obj

    def getObj(self):
        return self.obj

    def buildProtocol(self, addr):
        try:
            dbshow = self.dbinfo["db"]
            if "dbshow" in self.dbinfo:
                dbshow = self.dbinfo["dbshow"]
            p = DBClientProtocol(self.dbinfo["user"], 
                                 self.dbinfo["passwd"], 
                                 self.dbinfo["db"], 
                                 self.obj, 
                                 self.name, 
                                 dbshow)
            p.factory = self
            self.protocol = p
            return p
        except Exception, err:
            self.err(utils.cur(), err)

class RWClient(object):

    def __init__(self, dbinfo, obj, cb):
        self.dbinfo = dbinfo
        self.cb = cb
        self.protocolObj = obj
        self.ready = 0
        self.readClient = None
        self.writeClient = None
        self.readConn = None
        self.writeConn = None
        self.invoke_once = False
        self.checking = False
        self.raw_sql = []

        self.init()

    def decr(self):
        if self.ready > 0: self.ready -= 1

    def incr(self):
        self.ready += 1

    def changeDBInfo(self, host, key):
        if key == ZKConf.KEY_READ:
            self.dbinfo["host_r"] = host
        elif key == ZKConf.KEY_WRITE:
            self.dbinfo["host_w"] = host

    def getHost(self, key):
        if key == ZKConf.KEY_READ:
            return self.dbinfo["host_r"]
        elif key == ZKConf.KEY_WRITE:
            return self.dbinfo["host_w"]
        else:
            return ""

    def getDB(self):
        return self.dbinfo["dbshow"]

    def disconnect(self, host, key):
        try:
            self.changeDBInfo(host, key)
            utils.log(utils.cur(), "disconnect", key)
            if key == ZKConf.KEY_READ:
                self.init_diff(True)
            elif key == ZKConf.KEY_WRITE:
                self.init_diff(False)
        except Exception, err:
            utils.err(utils.cur(), err)

    def form_address(self, host):
        ADDR = namedtuple("ADDR", "host, port")
        host = host.split(":")
        if len(host) == 2:
            return ADDR(host[0], int(host[1]))
        else:
            return ADDR(host[0], 3306)

    def deal_conn_err(self, addr):
        # send error
        self.protocolObj.service_error(addr)
        # delete RWClient object
        self.protocolObj.factory.servers.remove(self.getDB(), self)
        utils.log(utils.cur(), self, id(self))
        # close RWClient object
        try:
            reactor.callLater(5, self.close_rw)
        except Exception, err:
            utils.err(utils.cur(), err)
        """
        close ServerProtocol object, then RWClient object is put back into
        server pool
        """
        self.protocolObj.close_proobj()

    def init_diff(self, isR = True):
        utils.log(utils.cur(), "in init_diff ready", self.ready)
        try:
            if isR:
                self.readClient.quit()
                del self.readClient, self.readConn
                self.readClient = DBClientFactory(self.dbinfo, 
                                                  self.protocolObj, 
                                                  self, 
                                                  "--read--")
                host, port = self.form_address(self.dbinfo["host_r"])
                utils.log(utils.cur(), host, port, '*'*8)
                self.readConn = reactor.connectTCP(host, port, self.readClient)
                utils.log(utils.cur(), host, port, '*'*8)
            else:
                self.writeClient.quit()
                del self.writeClient, self.writeConn
                self.writeClient = DBClientFactory(self.dbinfo, 
                                                   self.protocolObj, 
                                                   self, 
                                                   "--write--")
                host, port = self.form_address(self.dbinfo["host_w"])
                utils.log(utils.cur(), host, port, '*'*8)
                self.writeConn = reactor.connectTCP(host, port, self.writeClient)
                utils.log(utils.cur(), host, port, '*'*8)
            # check if connected
            reactor.callLater(ProxyServer.CONN_TIMEOUT, self.connectionFailed)
            utils.log(utils.cur(), 
                      self.readConn, 
                      self.readClient, 
                      self.writeConn, 
                      self.writeClient)
        except Exception, errmsg:
            utils.err(utils.cur(), "init_diff", errmsg)
            sys.exit(0)

    def init(self):
        utils.log(utils.cur(), "in init ready", self.ready)
        try:
            self.readClient = DBClientFactory(self.dbinfo, 
                                              self.protocolObj, 
                                              self,
                                              "--read--")
            host, port = self.form_address(self.dbinfo["host_r"])
            utils.log(utils.cur(), host, port)
            self.readConn = reactor.connectTCP(host, port, self.readClient)

            self.writeClient = DBClientFactory(self.dbinfo,
                                               self.protocolObj, 
                                               self,
                                               "--write--")
            host, port = self.form_address(self.dbinfo["host_w"])
            utils.log(utils.cur(), host, port)
            self.writeConn = reactor.connectTCP(host, port, self.writeClient)
            # check if connected
            reactor.callLater(ProxyServer.CONN_TIMEOUT, self.connectionFailed)
        except Exception, errmsg:
            utils.err(utils.cur(), errmsg)
            sys.exit(0)

    def connectionFailed(self):
        if self.ready != 2:
            self.quit_all()
            self.protocolObj.sendConnectionTimeout()

    def quit_all(self):
        try:
            for x in xrange(1):
                self.readClient.quit()
                time.sleep(0.01)
        except:
            pass
        try:
            for x in xrange(1):
                self.writeClient.quit()
                time.sleep(0.01)
        except:
            pass
        try:
            utils.log(utils.cur(), "lose connection")
            self.readClient.protocol.transport.loseConnection()
            self.readClient.stopFactory()
        except:
            pass
        try:
            utils.log(utils.cur(), "lose connection")
            self.writeClient.protocol.transport.loseConnection()
            self.writeClient.stopFactory()
        except:
            pass

    close_rw = quit_all

    def mysql_stmt_close(self, stmt_id):
        try:
            self.readClient.closeStmt(stmt_id)
            self.writeClient.closeStmt(stmt_id)
        except Exception, err:
            utils.err(utils.cur(), err)

    def raw_sql_clear(self):
        self.raw_sql = []

    @property
    def has_sql(self):
        return len(self.raw_sql) 

    def raw_sql_deal(self):
        raw_sql_copys = self.raw_sql[:]
        self.raw_sql = []
        while raw_sql_copys:
            name, rawsql = raw_sql_copys.pop(0)
            # utils.log(utils.cur(), name, rawsql)
            if name == self.readClient.protocol.name:
                self.readClient.protocol.queryRaw(rawsql)
            elif name == self.writeClient.protocol.name:
                self.writeClient.protocol.queryRaw(rawsql)

    def changeProtocolObj(self, obj):
        self.readClient.setObj(obj)
        self.writeClient.setObj(obj)

    def invoke(self):
        db = self.getDB()
        self.incr()
        utils.log(utils.cur(), "ready", self.ready, self.invoke_once)
        # initiate read/write connections successfully
        if not self.invoke_once and self.ready == 2:
            self.changeProtocolObj(self.protocolObj)
            self.protocolObj.factory.servers.push(db, self)
            if callable(self.cb): self.cb(self.protocolObj, db)
            self.invoke_once = True
        # deal SQL cache at first when connected
        elif self.invoke_once and self.ready == 2:
            utils.log(utils.cur(), "mei", len(self.raw_sql), self.raw_sql)
            self.raw_sql_deal()
