#!/usr/bin/env python
# -*- coding: utf-8 -*-
######################################################################
## Filename:      db_helper.py
##                
## Copyright (C) 2012,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Fri Mar  2 15:47:25 2012
##                
## Description:   
##                
######################################################################

import MySQLdb
from DBUtils.PooledDB import PooledDB
from twisted.enterprise import adbapi

import utils
from constants import DBConf

class DBPool(object):
    def __init__(self, dbconf, async=True):
        self.async = async
        try:
            if self.async:
                self.pool = adbapi.ConnectionPool("MySQLdb",
                                                  cp_min = DBConf.COMMONDB_MINCACHE,
                                                  cp_max = DBConf.COMMONDB_MAXCACHE,
                                                  cp_reconnect = True, # reconnect when failed
                                                  **dbconf)
            else:
                self.pool = PooledDB(MySQLdb,
                                 maxconnections = DBConf.COMMONDB_MAXCONNS,
                                 # setsession = ['set autocommit = 1'],
                                 use_unicode = DBConf.COMMONDB_UNICODE,
                                 mincached = DBConf.COMMONDB_MINCACHE,
                                 maxcached = DBConf.COMMONDB_MAXCACHE,
                                 blocking = DBConf.COMMONDB_BLOCKING,
                                 **dbconf)
        except Exception, err:
            utils.err(utils.cur(), err)

    def dealData(self, *args):
        utils.log(utils.cur(), args)

    def dealError(self, *args):
        utils.err(utils.cur(), args)

    def dealTrans(self, txn, *args):
        pass

    def execute(self, sql, cb=None, *args):
        if self.async:
            return self.execute_async(sql, cb, *args)
        else:
            return self.execute_sync(sql, cb, *args)

    def execute_async(self, sql, cb=None, *args):
        d = self.pool.runQuery(sql)
        d.addCallback(cb if callable(cb) else self.dealData, sql, *args)
        d.addErrback(self.dealError)

    def execute_sync(self, sql, cb=None, *args):
        try:
            conn = self.pool.connection()
            curr = conn.cursor()
            # utils.log(utils.cur(), sql)
            curr.execute(sql)
            data = curr.fetchall()
            curr.close()
            conn.close()
            if callable(cb):
                cb(data, sql, *args)
            else:
                self.dealData(data, sql, *args)
        except Exception, err:
            utils.err(utils.cur(), err)
            data = False
            if callable(cb):
                cb(data, sql, *args)
            else:
                self.dealData(data, sql, *args)

    def transaction(self, cb=None, *args):
        if self.async:
            self.transaction_async(cb, *args)
        else:
            self.transaction_sync(cb, *args)

    def transaction_async(self, cb=None, *args):
        d = self.pool.runInteraction(cb if callable(cb) else self.dealTrans, *args)
        d.addCallback(self.dealData)
        d.addErrback(self.dealError)

    def transaction_sync(self, cb=None, *args):
        cur = self.pool.connection().cursor()
        if callable(cb):
            cb(cur, *args)

    def close(self):
        self.pool.close()
