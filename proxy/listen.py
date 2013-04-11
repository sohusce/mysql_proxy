#!/usr/bin/env python
#-*- coding:utf-8 -*-
######################################################################
## Filename:      listen.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Tue Jun 19 13:47:45 2012
##                
## Description:   
##                
######################################################################
import os, sys, time, cjson, os.path, signal, collections
sys.path.append(os.getcwd())

from twisted.internet import protocol, reactor
from twisted.application import service, internet

import threads
from ServerProtocol import ServerProtocol
from RWClient import DBClientFactory, RWClient
from helper import utils, ip_helper, mq_helper, zk_helper
from helper.constants import *

class ServerFactory(protocol.Factory):

    def __init__(self, port):
        self.port = port
        self.conns = 0
        self.pool_size = 1
        self.servers = threads.ConnsPool("pool-connections")
        self.packets = None
        self.initPackets(self) # set packets
        self.ips = None # IpRangeList object
        self.dbs = []   # database name
        self.zk = None
        self.zk_dbinfo = {}

        self.forbidinfo = collections.defaultdict(dict)
        self.busy_proobj = collections.defaultdict(list)

        def _init_zk():
            # zk_helper
            self.zk = zk_helper.ZooKeeper("db_proxy", self.register_watches, "register_service", "proxy", self.port)

            self.zk_dbinfo = self.zk.get_dict()
            self.register_watches()

            self.servers.start()

            # mq_helper blocking thread
            self.mq = mq_helper.PikaRecvThread(servers=MQConf.SERVERS,
                                               exchange = MQConf.EXCHANGE_FORBID,
                                               queue = utils.getip() + MQConf.PROXY_SUFFIX,
                                               callback = self.mqCallback)
            self.mq.start()

            self.monitor = threads.ForbidMonitor(self)
            self.monitor.start()

            self.proxy_stats_log = threads.ProxyStatsLog(self.servers)
            self.proxy_stats_log.start()

        reactor.callLater(1, _init_zk)

    def reload_zkdbinfo(self):
        try:
            tmp = dict(self.zk_dbinfo)
            self.zk_dbinfo = self.zk.get_dict()
        except:
            self.zk_dbinfo = dict(tmp)

    def get_path(self, path, child=False):
        db_root_path = ZKConf.ZK_PATH_ROOT
        if path.startswith(db_root_path):
            path = path[len(db_root_path):]
        parts = path.split("/")
        tmp = dict(self.zk_dbinfo)
        for part in parts:
            if not part: continue
            if part in tmp:
                tmp = tmp[part]
            else:
                return False
        if child:
            if "__v__" in tmp: del tmp["__v__"]
            return tmp
        else:
            return tmp["__v__"]

    def register_watches(self):
        # 1. listen /database/db_info
        #    reset zkdbinfo
        #    close connections which are related to deleted nodes under db_info
        dbs = self.get_path(ZKConf.ZK_PATH_DB, child=True)
        self.zk.watch_child(ZKConf.ZK_PATH_DB, self.watcher_db_info)
        dbs = dbs.keys() if dbs else []
        self.dbs = dbs
        for db in dbs:
            path_authed_ip = os.path.join(ZKConf.ZK_PATH_DB, db, ZKConf.KEY_IP)
            # 2. listen /database/db_info/xxx/authed_ips
            #    reset zkdbinfo
            self.zk.watch_node(path_authed_ip, self.watcher_authed_ip)
            path_dbconf = os.path.join(ZKConf.ZK_PATH_DB, db, ZKConf.KEY_DBCONF)
            # 3. listen /database/db_info/xxx/dbconf/db_host_r
            #    listen /database/db_info/xxx/dbconf/db_host_w
            #    reset zkdbinfo
            #    reset busy_proobj rwclient connections
            #    reset idle_rwclient connections
            self.zk.watch_node(os.path.join(path_dbconf, ZKConf.KEY_READ), self.watcher_rw_ip)
            self.zk.watch_node(os.path.join(path_dbconf, ZKConf.KEY_WRITE), self.watcher_rw_ip)

        # 4. listen /database/authed_ips
        #    reset zkdbinfo
        #    reset self.ips
        ip_json = self.get_path(ZKConf.ZK_PATH_IPS)
        try:
            if ip_json:
                self.ips = ip_helper.IpRangeList(*tuple(cjson.decode(ip_json)))
        except Exception, err:
            utils.err(utils.cur(), err)
            self.ips = None
        self.zk.watch_node(ZKConf.ZK_PATH_IPS, self.watcher_ip)

        # 5. listen /database/forbid
        #    reset zkdbinfo
        #    reset self.forbidinfo
        dbs_forbid = self.get_path(ZKConf.ZK_PATH_FORBID, child=True)
        self.zk.watch_child(ZKConf.ZK_PATH_FORBID, self.watcher_forbid)
        dbs_forbid = dbs_forbid.keys() if dbs_forbid else []
        for db in dbs_forbid:
            try:
                data, meta = self.zk.get(os.path.join(ZKConf.ZK_PATH_FORBID, db), None)
                self.forbidinfo[db] = cjson.decode(data)
            except Exception, err:
                utils.err(utils.cur(), err)

    def watcher_db_info(self, event, true_path):
        self.reload_zkdbinfo()
        dbs = self.get_path(true_path, child=True)
        dbs = dbs.keys() if dbs else []
        # 1.watch nodes when db_info nodes are new
        newdbs = list(set(dbs) - set(self.dbs))
        for db in newdbs:
            path_authed_ip = os.path.join(ZKConf.ZK_PATH_DB, db, ZKConf.KEY_IP)
            # 2. listen /database/db_info/xxx/authed_ips
            #    reset zkdbinfo
            self.zk.watch_node(path_authed_ip, self.watcher_authed_ip)
            path_dbconf = os.path.join(ZKConf.ZK_PATH_DB, db, ZKConf.KEY_DBCONF)
            # 3. listen /database/db_info/xxx/dbconf/db_host_r
            #    reset /database/db_info/xxx/dbconf/db_host_w
            #    reset zkdbinfo
            #    reset busy_proobj rwclient connections
            #    reset idle_rwclient connections
            self.zk.watch_node(os.path.join(path_dbconf, ZKConf.KEY_READ), self.watcher_rw_ip)
            self.zk.watch_node(os.path.join(path_dbconf, ZKConf.KEY_WRITE), self.watcher_rw_ip)
                
        # 2.close connections when db_info nodes are deleted
        diff = list(set(self.dbs) - set(dbs))
        utils.log(utils.cur(), self.dbs, dbs, diff)
        for db in diff:
            if db in self.servers:
                self.servers[db].close_conns()
        self.dbs = dbs

    def watcher_authed_ip(self, event, true_path):
        self.reload_zkdbinfo()

    def watcher_rw_ip(self, event, true_path):
        self.reload_zkdbinfo()
        host = self.get_path(true_path)
        paths = true_path.split("/")
        db, rw_type = paths[3], paths[-1]
        # 1. reset idle_rwclient connections
        backends = self.servers[db] if db in self.servers else []
        utils.log(utils.cur(), true_path, host, backends.copy() if backends else [], len(backends))

        # reset connections when new IP and old IP are different
        for rwclient, pushtime in (backends.copy() if backends else []):
            oldhost = rwclient.getHost(rw_type)
            utils.log(utils.cur(), oldhost, host)
            if oldhost and oldhost != host:
                rwclient.disconnect(host, rw_type)
            
        # 2. reset busy_proobj rwclient connections
        utils.log(utils.cur(), self.busy_proobj)
        # reset connections when new IP and old IP are different
        for proobj in self.busy_proobj[db]:
            if proobj.server:
                oldhost = proobj.server.getHost(rw_type)
                if oldhost and oldhost != host:
                    utils.log(utils.cur(), oldhost, host)
                    proobj.server.disconnect(host, rw_type)

    def watcher_ip(self, event, true_path):
        self.reload_zkdbinfo()
        ip_json = self.get_path(true_path)
        try:
            self.ips = ip_helper.IpRangeList(*tuple(cjson.decode(ip_json)))
        except Exception, err:
            utils.err(utils.cur(), err)
            self.ips = None
        
    def watcher_forbid(self, event, true_path):
        self.reload_zkdbinfo()
        dbs_forbid = self.get_path(true_path, child=True)
        forbidinfo = collections.defaultdict(dict)
        for db in dbs_forbid:
            try:
                forbid_db = self.get_path(os.path.join(true_path, db))
                forbidinfo[db] = cjson.decode(forbid_db)
            except Exception, err:
                utils.err(utils.cur(), err)
        self.forbidinfo = forbidinfo

    """
    get binary data of ServerFactory for client authentication
    """
    def initPackets(self, server_factory):
        dbinfo = DBConf.COMMONDB
        tmpFac = DBClientFactory(dbinfo, server_factory)
        reactor.connectTCP(dbinfo["host"], dbinfo["port"], tmpFac)

    def getHandshakeRaw(self):
        return self.packets["handshake_raw"]

    def getHandshakeDic(self):
        return self.packets["handshake_dic"]

    def mqCallback(self, channel, method_frame, header_frame, body):
        try:
            if not self.zk.is_proxy_master(): return
            # master's business
            data_dict = cjson.decode(body)
            # ** MUST ** ack
            channel.basic_ack(method_frame.delivery_tag)
            utils.log(utils.cur(), body, data_dict)
            if not isinstance(data_dict, dict):
                return
            for db, forbid in data_dict.iteritems():
                if not forbid[Forbid.KEY_TYPE] in (Forbid.FORBID_WORKING, Forbid.FORBID_FOREVER):
                    return
                forbid[Forbid.KEY_START] = time.time()
                path = os.path.join(ZKConf.ZK_PATH_FORBID, db)
                orig = self.get_path(path)
                if orig is False:
                    self.zk.mknode(path, cjson.encode(forbid))
                else:
                    old = cjson.decode(orig)
                    if old[Forbid.KEY_TYPE] == forbid[Forbid.KEY_TYPE] and \
                            old[Forbid.KEY_TYPE] == Forbid.FORBID_WORKING and \
                            old[Forbid.KEY_START] + old[Forbid.KEY_DURATION] > time.time():
                        utils.log(utils.cur(), "still forbidding")
                    else:
                        utils.log(utils.cur(), "change forbid")
                        # change /database/forbid/db
                        self.forbidinfo[db] = forbid
                        self.zk.set(path, cjson.encode(forbid))
        except Exception, err:
            utils.err(utils.cur(), err)

    def getServer(self, db, callback, proObj):
        if db in self.servers and len(self.servers[db]):
            if callable(callback):
                utils.log(utils.cur(), "use old RWClient")
                try:
                    proObj.server = self.servers.pop(db)
                    proObj.server.changeProtocolObj(proObj)
                    self.busy_proobj[db].append(proObj)
                except Exception, err:
                    return proObj.pool_error(err)
                callback(proObj, True)
        else:
            utils.log(utils.cur(), "create new RWClient")
            dbpath = os.path.join(ZKConf.ZK_PATH_DB, db, ZKConf.KEY_DBCONF)
            if self.zk.exists(dbpath, None):
                try:
                    stats_conns.check(db)
                except Exception, err:
                    utils.log(utils.cur(), err)
                    return proObj.pool_error(err)

                def cb(pro, db):
                    if callable(callback):
                        proObj.server = pro.factory.servers.pop(db)
                        self.busy_proobj[db].append(proObj)
                        callback(proObj)
                
                dbinfo = {
                    "host_r" : "",
                    "host_w" : "",
                    "user"   : "",
                    "passwd" : "",
                    "dbshow" : db,
                    "db" : "",
                    }
                
                path_host_r = os.path.join(dbpath, ZKConf.KEY_READ)
                path_host_w = os.path.join(dbpath, ZKConf.KEY_WRITE)
                path_pass   = os.path.join(dbpath, ZKConf.KEY_PASSWORD)
                path_user   = os.path.join(dbpath, ZKConf.KEY_USER)
                path_db     = os.path.join(dbpath, ZKConf.KEY_DB)
                dbinfo["host_r"] = self.zk.get(path_host_r, None)[0]
                dbinfo["host_w"] = self.zk.get(path_host_w, None)[0]
                dbinfo["passwd"] = utils.decrypt(self.zk.get(path_pass, None)[0])
                dbinfo["user"] = self.zk.get(path_user, None)[0]
                dbinfo["db"] = self.zk.get(path_db, None)[0]
                utils.log(utils.cur(), path_host_r, path_host_w)
                RWClient(dbinfo, proObj, cb)

            else:
                raise Exception("MySQL has gone away, f_f")
    
    def takeServer(self, server, proObj):
        if server and hasattr(server, "ready") and server.ready == 2:
            # clean SQL cache
            server.raw_sql_clear()
            db = server.getDB()
            stats_conns.decr(db)
            try:
                self.busy_proobj[db].remove(proObj)
            except:
                pass
            server.changeProtocolObj(None)
            self.servers.push(server.getDB(), server)
            utils.log(utils.cur(), stats_conns, self.servers)
    
    def buildProtocol(self, addr):
        self.conns += 1
        p = ServerProtocol()
        p.factory = self
        return p

    def close_free_servers(self):
        for _, objs in self.busy_proobj.iteritems():
            while objs:
                proobj = objs.pop(0)
                stats_conns.decr(proobj.database)
                proobj.close_proobj()
                del proobj

    def stopFactory(self):
        utils.log(utils.cur(), "dbproxy stopping")
        self.zk.close()
        self.mq.stop()
        self.monitor.stop()
        self.proxy_stats_log.stop()
        # time.sleep(0.5)
        self.close_free_servers()
        self.servers.stop()
        stats_conns.close()

utils.gen_logger(ProxyServer.LOGGER, ProxyServer.LOGFILE)
utils.parse_args(sys.argv[1:])
f = ServerFactory(ProxyServer.PORT)

def handler(sig, action):
    from objects_dump import MemObject
    MemObject.dump("/opt/logs/sce_db/dbproxy.txt")

signal.signal(signal.SIGUSR1, handler)
if __name__ == "__main__":
    reactor.listenTCP(ProxyServer.PORT, f)
    reactor.run()
else:
    uid, gid = utils.getuidgid()
    application = service.Application(ProxyServer.APPLICATION, uid=uid, gid=gid)
    internet.TCPServer(ProxyServer.PORT, f).setServiceParent(application)
