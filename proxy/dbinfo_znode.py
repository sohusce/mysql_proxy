#!/usr/bin/env python
#-*- coding: utf-8 -*-
######################################################################
## Filename:      dbinfo_znode.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Fri Mar  2 15:05:01 2012
##                
## Description:   
##                
######################################################################
import zookeeper, cjson, types, os.path, sys
from collections import namedtuple

from mysql_packet import mysqlsha1
from helper import zk_helper, db_helper, utils
from helper.constants import ZKConf, DBConf, Quota

class DBInfo(object):
    def __init__(self):
        self.zk = zk_helper.ZooKeeper("dbinfo")
        self.dbinfo = db_helper.DBPool(DBConf.APPENGINEDB, async=False)
        self.rootpath = ZKConf.ZK_PATH_DB
        self.zk.mknode(self.rootpath)

    def start(self):
        def _exec(dblist, sql):
            LOGCONFIG = namedtuple("LOGCONFIG", "id,dbname,dbtype,dbdisabled,username,password,host_r,host_w,ips,dbnode")
            for row in map(LOGCONFIG._make, dblist):
                utils.log(utils.cur(), row)
                dbpath = os.path.join(self.rootpath, row.dbnode)
                dbroot = self.zk.exists(dbpath, None)
                if not dbroot:
                    self.zk.create(dbpath, 
                                   "",
                                   [ZKConf.ZOO_CREATOR_ALL_ACL],
                                   0)
                    self.create_leafs(row, self.rootpath + "/" + row.dbnode)
        self.dbinfo.execute("SELECT * FROM `%s`" % DBConf.TABLE_DBINFO, _exec)

    def create_dbinfo(self, dbinfo):
        if "dbnode" not in dbinfo:
            raise Exception("dbnode **MUST** contain")
        dbnode = dbinfo["dbnode"]
        if not dbnode:
            raise Exception("dbnode **MUST NOT** null")
    
        def _exec(nodes, sql):
            if nodes and len(nodes):
                raise Exception("dbnode '%s' exists" % dbnode)
        sql = """SELECT `dbnode` 
                 FROM `%s` 
                 WHERE `dbnode` = '%s'""" % (DBConf.TABLE_DBINFO, dbnode)
        self.dbinfo.execute(sql, _exec)

        # insert into mysql
        def _trans(cur, *args):
            try:
                keys = ",".join(["`%s`" % k for k in dbinfo.keys()])
                vals = ",".join(["'%s'" % v for v in dbinfo.values()])
                sql = "INSERT INTO `%s` (%s) VALUES(%s)" % (DBConf.TABLE_DBINFO,
                                                            keys, 
                                                            vals)
                utils.log(utils.cur(), keys, vals, sql)
                cur.execute(sql)
                cur.execute("COMMIT")
            except Exception, e:
                cur.execute("ROLLBACK")
                utils.err(utils.cur(), e)
                raise Exception(e)
        self.dbinfo.transaction(_trans)

        # insert into zookeeper
        DBINFO = namedtuple("DBINFO", "dbname,dbtype,dbdisabled,username,password,host_r,host_w,ips,dbnode")
        row = None
        try:
            row = DBINFO(**dbinfo)
            utils.log(utils.cur(), row)
        except Exception, e:
            raise Exception(e)

        dbpath = os.path.join(self.rootpath, row.dbnode)
        dbroot = self.zk.exists(dbpath, None)
        if not dbroot:
            self.zk.create(dbpath, 
                           "",
                           [ZKConf.ZOO_CREATOR_ALL_ACL],
                           0)
            self.create_leafs(row, self.rootpath + "/" + row.dbnode)

        

    def create_leafs(self, log, dbpath):
        dbinfo = {
            "db_disabled" : log.dbdisabled,
            "authed_ips" : log.ips,
            "dbconf"  : {
                "db_user" : log.username,
                "db_password" : log.password,
                "db_db"   : log.dbname,
                "db_host_r" : log.host_r,
                "db_host_w" : log.host_w,
                },
            "dbtype"       : log.dbtype
            }

        def _create(info, path):
            for key,value in info.iteritems():
                subpath = "".join([path, "/", key])
                subnode = self.zk.exists(subpath, None)
                if not subnode:
                    if type(value) is not dict:
                        utils.log(utils.cur(), subpath, value)
                        self.zk.create(subpath, 
                                       str(value),
                                       [ZKConf.ZOO_CREATOR_ALL_ACL],
                                       0)
                    else:
                        self.zk.create(subpath,
                                       "",
                                       [ZKConf.ZOO_CREATOR_ALL_ACL],
                                       0)
                        _create(value, subpath)
        
        _create(dbinfo, dbpath)

class DBType(object):
    def __init__(self):
        self.zk = zk_helper.ZooKeeper("dbtype")
        self.rootpath = ZKConf.ZK_PATH_TYPE
        self.dbtype = db_helper.DBPool(DBConf.APPENGINEDB, async=False)
        self.zk.mknode(self.rootpath)

    def sync_mysql(self):
        root = ZKConf.ZK_PATH_DB
        path = {
            "dbname": "dbconf/db_db",
            "dbtype": "dbtype",
            "dbdisabled": "db_disabled",
            "username": "dbconf/db_user",
            "password": "dbconf/db_password",
            "host_r": "dbconf/db_host_r",
            "host_w": "dbconf/db_host_w",
            "ips": "authed_ips"
            }
        dbnodes = self.zk.get_children(root, None)
        utils.log(utils.cur(), dbnodes)

        def _trans(cur, *args):
            def _exec(data, record):
                if len(data):
                    sql = ",".join(["`%s`='%s'" % (k,v) for k,v in record.items()])
                    sql = "UPDATE `%s` SET %s WHERE `dbnode`='%s'" % (DBConf.TABLE_DBINFO, sql, dbnode)
                    utils.log(utils.cur(), sql)
                    cur.execute(sql)
                else:
                    record["dbnode"] = dbnode
                    keys = ",".join(["`%s`" % k for k in record.keys()])
                    vals = ",".join(["'%%(%s)s'" % v for v in record.keys()])
                    vals = vals % record
                    sql = "INSERT INTO `%s` (%s) VALUES (%s)" % (DBConf.TABLE_DBINFO, keys, vals)
                    utils.log(utils.cur(), sql)
                    cur.execute(sql)
            cur.execute("START TRANSACTION")
            for dbnode in dbnodes:
                if not dbnode: continue
                record = dict()
                for key, value in path.items():
                    node = os.path.join(root, dbnode, value)
                    if self.zk.exists(node, None):
                        (data, meta) = self.zk.get(node, None)
                        record[key] = data

                cur.execute("SELECT * FROM `%s` WHERE `dbnode` = '%s'" % (DBConf.TABLE_DBINFO, dbnode))
                dbinfo = cur.fetchall()
                _exec(dbinfo, record)
            # delete records that have been already deleted
            sql = "DELETE FROM `%s` WHERE `dbnode` not in (%s) " % (DBConf.TABLE_DBINFO, ",".join(["'"+d+"'" for d in dbnodes]))
            utils.log(utils.cur(), sql)
            cur.execute(sql)
            cur.execute("COMMIT")

        def _transaction(cur, *args):
            try:
                _trans(cur, *args)
            except Exception, err:
                cur.execute("ROLLBACK")
                utils.log(utils.cur(), err)

        self.dbtype.transaction(_transaction)

    def delnode(self, path):
        self.zk.delnode(path)

    def list(self, *paths):
        for path in paths:
            self.zk.list(path)
        
    def write(self, dbtype):
        self.create_node(dbtype, 1, "default")
        for i in xrange(1, 4):
            self.create_node(dbtype, i, "level" + str(i))
        self.create_node(dbtype, 10, ZKConf.INTERNAL)

    def create_node(self, orig_dbtype, mul, nodename):
        if not isinstance(mul, (int, long)): return False
        dbtype = dict(orig_dbtype)
        if mul > 1:
            for k in dbtype:
                if type(dbtype[k]) in (int, long):
                    dbtype[k] = dbtype[k] * mul
        nodepath = os.path.join(self.rootpath, nodename)
        if not self.zk.exists(nodepath, None):
            self.zk.mknode(nodepath, cjson.encode(dbtype))
        dbtype["dbtype"] = nodename
        dbtype["table"] = DBConf.TABLE_DBTYPE
        def _trans(cur, *args):
            cur.execute("START TRANSACTION")
            try:
                cur.execute("SELECT * FROM `%(table)s` WHERE dbtype='%(dbtype)s'" % dbtype)                
                dbnums = cur.fetchall()
                if len(dbnums) == 0:
                    insert_sql = """INSERT INTO `%(table)s` (`dbtype`, `maxconn`, `disk`, `network`, `slowquery_duration`, `slowquery_times`, `tablenums`, `tablerows`, `tablesize`, `cputime`) VALUES ('%(dbtype)s', %(maxconn)d, %(disk)d, %(network)d, %(slowquery_duration)d, %(slowquery_times)d, %(tablenums)d, %(tablerows)d, %(tablesize)d, %(cputime)d)""" % dbtype
                    utils.log(utils.cur(), insert_sql)
                    cur.execute(insert_sql)
                cur.execute("COMMIT")
            except Exception, err:
                utils.log(utils.cur(), err)
                cur.execute("ROLLBACK")
        self.dbtype.transaction(_trans)

def init_znode():
    # 1. /database/db_type
    DBType().write(Quota.Keys)
    # 2. /database/db_info
    DBInfo().start()
    # 3. /database/authed_ips
    zk = zk_helper.ZooKeeper("init_znode")
    # zk.connect()
    # zk.wait_until_connected()

    zk.mknode(ZKConf.ZK_PATH_IPS, cjson.encode([utils.getip()]))
    # 3. /database/forbid
    zk.mknode(ZKConf.ZK_PATH_FORBID, "")
    # 4. /database/temp_master
    zk.mknode(ZKConf.ZK_PATH_TEMP_MASTER, "")
    # 5. /database/temp_stats
    zk.mknode(ZKConf.ZK_PATH_TEMP_STATS, "")
    # 6. /database/temp_proxy
    zk.mknode(ZKConf.ZK_PATH_TEMP_PROXY, "")
    # 7. /database/leader/db_stats
    zk.mknode(ZKConf.ZK_PATH_LEADER_DB_STATS, "")
    # 8. /database/leader/db_master
    zk.mknode(ZKConf.ZK_PATH_LEADER_DB_MASTER, "")

def sync_mysql():
    DBType().sync_mysql()

def init_a_node(dbinfo):
    template = {
        "dbname": "",
        "dbnode": "",
        "dbtype": "",
        "dbdisabled": "",
        "username": "",
        "password": "",
        "host_r": "",
        "host_w": "",
        "ips": ""
        }
    dbinfo["password"] = utils.encrypt(dbinfo["password"])
    DBInfo().create_dbinfo(dbinfo)

if __name__ == "__main__":
    utils.gen_logger("dbinfo_znode_logger", "/tmp/l.log")
    options = ["init_znode", "sync_mysql", "init_a_node"]
    if len(sys.argv) < 2:
        utils.log(utils.cur(), "Usage: python %s %s arguments" % (sys.argv[0], "/".join(options)))
        sys.exit(0)
    arg1 = sys.argv[1]
    if not arg1 in options:
        utils.log(utils.cur(), "Usage: python %s %s arguments" % (sys.argv[0], "/".join(options)))
        sys.exit(0)

    utils.parse_args(sys.argv[2:])
    if arg1 == "init_znode":
        # init_znode()
        # print DBType().zk.get_dict("/database")
        DBType().list("/database/db_info/appstat")
        # DBType().zk.set("/database/db_info/appstat/dbconf/db_host_r", "10.11.150.126:3306")
        # DBType().zk.set("/database/db_info/appengine/dbconf/db_host_w", "10.11.150.114:3306")
        # DBType().zk.set("/database/db_info/appengine/dbconf/db_host_r", "10.11.150.114:3306")
    elif arg1 == "sync_mysql":
        sync_mysql()
    else:
        dbinfo = {
            "dbname": "test",           # /database/db_info/xxx/dbconf/db_db
            "dbnode": "test",           # /database/db_info/xxx
            "dbtype": "internal",           # /database/db_info/xxx/dbtype
            "dbdisabled": "0",       # /database/db_info/xxx/db_disabled
            "username": "root",         # /database/db_info/xxx/dbconf/db_user
            "password": "root",         # /database/db_info/xxx/dbconf/db_password plain text
            "host_r": "10.10.10.10:3306",           # /database/db_info/xxx/dbconf/db_host_r
            "host_w": "10.10.10.11:3306",           # /database/db_info/xxx/dbconf/db_host_w
            "ips": "[]"             # /database/db_info/xxx/authed_ips JSON
            }
        init_a_node(dbinfo)
