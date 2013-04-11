#!/usr/bin/env python
# -*- coding: utf-8 -*-
######################################################################
## Filename:      threads.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Wed Jun 20 14:30:40 2012
##                
## Description:   
##                
######################################################################

import time, random, threading

from helper import utils, db_helper
from helper.constants import WAIT_TIMEOUT, Forbid, DBConf, ProxyServer

class DBPool(object):
    """
    connection pool
    """
    def __init__(self, dbname):
        self.dbname = dbname
        self._idle_cache = []
        self._mincached = ProxyServer.POOL_MIN
        self._maxcached = ProxyServer.POOL_MAX
        self._idle_time = ProxyServer.POOL_IDLE
        self._condition = threading.Condition()

    def pop(self):
        try:
            con, pushtime = self._idle_cache.pop(0)
        except Exception, err:
            raise utils.NoConnections(self.dbname)
        return con

    def push(self, con):
        self._condition.acquire()
        con_pushtime = (con, int(time.time()))
        self._idle_cache.append(con_pushtime)
        self._condition.notifyAll()
        self._condition.release()

    def remove(self, aging_con):
        self._condition.acquire()
        for con, pushtime in self._idle_cache[:]:
            if id(aging_con) == id(con):
                try:
                    self._idle_cache.remove((aging_con, pushtime))
                except:
                    pass
        self._condition.notifyAll()
        self._condition.release()

    def clear(self, allclear=False):
        self._condition.acquire()
        for con, pushtime in self._idle_cache:
            con.checking = True
        if allclear:
            while self._idle_cache:
                try:
                    con, pushtime = self._idle_cache.pop(0)
                    con.close_rw()
                    del con
                except:
                    pass
        else:
            tmp_idle_cache = []
            while self._idle_cache:
                try:
                    length = len(self._idle_cache)
                    # not thread-safe
                    con, pushtime = self._idle_cache.pop(0)
                    if int(time.time()) - pushtime > self.idle_time:
                        # reset con_pushtime
                        if length <= self._mincached:
                            con.checking = False
                            con_pushtime = (con, int(time.time()))
                            tmp_idle_cache.append(con_pushtime)
                        # delete connection
                        else:
                            con.close_rw()
                            del con
                    else:
                        con.checking = False
                        tmp_idle_cache.append((con, pushtime))
                except:
                    pass
            self._idle_cache = tmp_idle_cache[:]
            del tmp_idle_cache
        self._condition.notifyAll()
        self._condition.release()

    def close_conns(self):
        self.clear(True)

    __del__ = close_conns

    @property
    def clearing(self):
        return len(self._idle_cache)

    @property
    def idle_time(self):
        return self._idle_time

    @property
    def condition(self):
        return self._condition

    def __len__(self):
        return len(self._idle_cache)

    def __iter__(self):
        return iter(self._idle_cache)

    def copy(self):
        return self._idle_cache[:]

    def show(self):
        return (self.dbname, len(self._idle_cache))

class ConnsPool(threading.Thread):
    """
    proxy-->server connection pool
    """
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        self._condition = threading.Condition()
        self.conns = {}
        self.running = True

    def push(self, db, con):
        if not db: return
        self._condition.acquire()
        if not db in self.conns:
            self.conns[db] = DBPool(db)
        self.conns[db].push(con)
        self._condition.notify()
        self._condition.release()

    def pop(self, db):
        if not db: return
        self._condition.acquire()
        if not db in self.conns:
            self.conns[db] = DBPool(db)
        self._condition.notify()
        self._condition.release()
        return self.conns[db].pop()

    def remove(self, db, con):
        if not db: return
        self._condition.acquire()
        if not db in self.conns:
            self.conns[db] = DBPool(db)
        self.conns[db].remove(con)
        self._condition.notify()
        self._condition.release()

    def clear(self):
        now = time.time()
        for db, con in self.conns.iteritems():
            if con.clearing: con.clear()

    def run(self):
        while self.running:
            self.clear()
            time.sleep(random.uniform(2, 3))

    def stop(self):
        if self.isAlive():
            utils.log(utils.cur(), 'stop thread %s' % self.name)
            self.close_pool()
            self.running = False

    def close_pool(self):
        for db, con in self.conns.iteritems():
            if con.clearing: con.close_conns()
        
    def show(self):
        conns = []
        for db, con in self.conns.iteritems():
            conns.append(con.show())
        return conns

    def __iter__(self):
        return iter(self.conns)

    def __contains__(self, db):
        return db in self.conns and len(self.conns[db])

    def __getitem__(self, db):
        return self.conns[db] if db in self.conns else None

    def __str__(self):
        conns = {}
        for db, con in self.conns.iteritems():
            conns[db] = con.show()
        return str(conns)

class ProxyStatsLog(threading.Thread):

    def __init__(self, servers):
        threading.Thread.__init__(self)
        self.running = True
        self.ip_addr = utils.getip()
        self.servers = servers
        self.traffic_db = db_helper.DBPool(DBConf.COMMONDB, async=True)
        self.traffic_db.execute("SET AUTOCOMMIT = 1")

    def insert(self):
        has_more = True
        stats = dict()
        while has_more:
            try:
                arr = proxy_stats.get_nowait()
                if len(arr) == 4:
                    key = arr[0]
                    stats[key] = stats[key] if key in stats else [0, 0, 0]
                    stats[key][0] += int(arr[1])
                    stats[key][1] += int(arr[2])
                    stats[key][2] += int(arr[3])
            except Exception, err:
                has_more = False

        ip_now = [self.ip_addr, time.strftime("%Y-%m-%d %H:%M:00")]
        for dbname, arr in stats.iteritems():
            if len(arr) != 3: continue
            values = [DBConf.TABLE_STAT, dbname]
            values.extend(ip_now)
            values.extend(arr)
            client_conns = stats_conns.get(dbname)
            cached_conns = self.servers[dbname]
            cached_conns = len(cached_conns) if cached_conns else 0
            values.extend([client_conns, client_conns + cached_conns])
            stats_sql = """INSERT INTO `%s` 
                  (`db_name`, `db_proxy`, 
                   `stattime`, `requests`, 
                   `traffic_in`, `traffic_out`, 
                   `client_conns`, `proxy_conns`) 
                  VALUES ('%s', '%s', '%s', %d, %d, %d, %d, %d)"""  % tuple(values)
            self.traffic_db.execute(stats_sql)

    def run(self):
        while self.running:
            if int(time.time()) % 60 == 30:
                self.insert()
            time.sleep(random.uniform(0.8, 1))

    def stop(self):
        if self.isAlive():
            self.running = False
            self.traffic_db.close()

class ProxyStats(threading.Thread):
    """
    statistics of traffic-in, traffic-out, # of connections
    """
    def __init__(self, name, dbname):
        threading.Thread.__init__(self)
        self.dbname = dbname
        self.running = True
        self.requests = 0
        self.traffic_in = 0
        self.traffic_out = 0

    def grow(self, traffic, isP2C=True):
        if isP2C:
            self.traffic_in += traffic
        else:
            self.requests += 1
            self.traffic_out += traffic

    def reset(self):
        self.requests = 0
        self.traffic_in = 0
        self.traffic_out = 0

    def insert_queue(self):
        try:
            stats_arr = [self.dbname, 
                         self.requests,
                         self.traffic_in, 
                         self.traffic_out]
            self.reset()
            proxy_stats.put_nowait(stats_arr)
        except Exception, err:
            pass
        
    def write(self):
        if self.traffic_in or self.traffic_out or self.requests:
            self.insert_queue()

    def check_traffic(self):
        if not int(time.time()) % 60:
            self.insert_queue()

    def run(self):
        while self.running:
            self.check_traffic()
            time.sleep(random.uniform(0.8, 1))

    def stop(self):
        if self.isAlive():
            self.running = False


class WaitTimeout(threading.Thread):
    """
    check whether client connections are idle. If so, close connections.
    """
    def __init__(self, cb, proobj, name = "thread wait_timeout"):
        threading.Thread.__init__(self)
        self.name = name
        self.starttime = time.time()
        self.running = True
        self.cb = cb
        self.obj = proobj

    def reset(self):
        self.starttime = time.time()
        
    def check(self):
        if time.time() - self.starttime > WAIT_TIMEOUT: 
            if hasattr(self.obj, "server") and self.obj.server.has_sql:
                self.reset()
            else:
                self.stop()

    def run(self):
        while self.running:
            self.check()
            time.sleep(random.uniform(1,2))

    def stop(self):
        if self.isAlive():
            self.running = False
            if callable(self.cb): self.cb()


class ForbidMonitor(threading.Thread):
    def __init__(self, serverfactory, name="thread forbid monitor"):
        threading.Thread.__init__(self)
        self.sf = serverfactory
        self.name = name
        self.running = True

    def check(self):
        forbidinfo = dict(self.sf.forbidinfo)
        for db, info in forbidinfo.iteritems():
            if not info[Forbid.KEY_TYPE] in (Forbid.FORBID_WORKING, Forbid.FORBID_FOREVER):
                utils.log(utils.cur(), "erase forbid", db)
                self.sf.zk.erase_forbid(db)

            if info[Forbid.KEY_TYPE] == Forbid.FORBID_WORKING and \
                    info[Forbid.KEY_START] + info[Forbid.KEY_DURATION] < time.time():
                utils.log(utils.cur(), "erase forbid", db)
                self.sf.zk.erase_forbid(db)

    def run(self):
        while self.running:
            self.check()
            time.sleep(random.uniform(1, 1.5))

    def stop(self):
        if self.isAlive():
            utils.log(utils.cur(), 'stop thread %s' % self.name)
            self.running = False
