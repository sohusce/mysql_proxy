#!/usr/bin/env python
#-*- coding: utf-8 -*-
######################################################################
## Filename:      zk_helper.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Wed Feb 29 15:43:18 2012
##                
## Description:   ZooKeeper operations
##                
######################################################################
from constants import ZKConf, Forbid
import utils

import zookeeper
import sys, os, cjson, time, os.path
import threading, traceback
from collections import namedtuple
from twisted.internet import reactor

# Mapping of connection state values to human strings.
STATE_NAME_MAPPING = {
    zookeeper.ASSOCIATING_STATE: "associating",
    zookeeper.AUTH_FAILED_STATE: "auth-failed",
    zookeeper.CONNECTED_STATE: "connected",
    zookeeper.CONNECTING_STATE: "connecting",
    zookeeper.EXPIRED_SESSION_STATE: "expired",
    }

# Mapping of event type to human string.
TYPE_NAME_MAPPING = {
    zookeeper.NOTWATCHING_EVENT: "not-watching",
    zookeeper.SESSION_EVENT: "session",
    zookeeper.CREATED_EVENT: "created",
    zookeeper.DELETED_EVENT: "deleted",
    zookeeper.CHANGED_EVENT: "changed",
    zookeeper.CHILD_EVENT: "child", 
    }

class ClientEvent(namedtuple("ClientEvent", 'type, connection_state, path')):
    """
    A client event is returned when a watch deferred fires. It denotes
    some event on the zookeeper client that the watch was requested on.
    """
    @property
    def type_name(self):
        return TYPE_NAME_MAPPING[self.type]

    @property
    def state_name(self):
        return STATE_NAME_MAPPING[self.connection_state]

    def __repr__(self):
        return  "<ClientEvent %s at %r state: %s>" % (self.type_name, 
                                                      self.path, 
                                                      self.state_name)

def watchmethod(func):
    def decorated(handle, event, state, path):
        event = ClientEvent(event, state, path)
        return func(event)
    return decorated

class ZooKeeperError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class Connection:
    def __init__(self, name, register_watch_cb, first, register_node_cb, *args, **kwargs):
        # zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
        self.register_watch_cb = register_watch_cb
        self.register_node_cb = register_node_cb
        self._first = first
        self._name = name
        self._args = args
        self._kwargs = kwargs
        self._handler = None
        self._conn_cv = threading.Condition()
        self.connect()

    @property
    def handler(self):
        return self._handler

    def connect(self):
        self._connected = False
        self.safe_connect()
        if not self._connected:
            raise ZooKeeperError("zookeeper [ %s ] connect() failed" % str(ZKConf.ZK_HOST))

    def safe_connect(self):
        self._conn_cv.acquire()
        self._handler = zookeeper.init(ZKConf.ZK_HOST, self.watcher_conn, ZKConf.ZOO_SESSION_TIMEOUT) # milliseconds
        self._conn_cv.wait(8.0)
        self._conn_cv.release()

        try_times = 10
        while not self._connected and try_times > 0:
            self.connect()
            try_times -= 1

        if not self._connected:
            raise ZooKeeperError("zookeeper [ %s ] safe_connect() failed" % str(ZKConf.ZK_HOST))

        try:
            user_pass = "%s:%s" % (ZKConf.ZOO_USERNAME, ZKConf.ZOO_PASSWORD)
            utils.log(utils.cur(), self._name, "--add_auth--")
            zookeeper.add_auth(self._handler, "digest", user_pass, None)
            if self.register_node_cb:
                try:
                    func = getattr(self, self.register_node_cb)
                    if func and callable(func):
                        utils.log(utils.cur(), self.register_node_cb, "%s callback %s" % ("*"*10, "*"*10))
                        def _do_register_node():
                            func(*self._args, **self._kwargs)
                        _do_register_node()
                        """
                        check whether temporary nodes are still existed
                        when session timeout elapsed
                        because there are old temporary nodes when register new
                        temporary nodes
                        """
                        threading.Timer(int(ZKConf.ZOO_SESSION_TIMEOUT/1000), _do_register_node).start()
                except AttributeError, err:
                    utils.err(utils.cur(), traceback.format_exc())

            utils.log(utils.cur(), self._first, self.register_watch_cb)
            if not self._first:
                if self.register_watch_cb:
                    try:
                        utils.log(utils.cur(), "ready to callback")
                        self.register_watch_cb()
                    except Exception, err:
                        utils.err(utils.cur(), traceback.format_exc())
            else:
                self._first = False
        except Exception, err:
            utils.err(utils.cur(), err)

    def watcher_conn(self, handler, mode, state, path):
        self._conn_cv.acquire()
        event = ClientEvent(mode, state, path)
        if state == zookeeper.EXPIRED_SESSION_STATE:
            utils.log(utils.cur(), self._name, event.state_name)
            try:
                zookeeper.close(self._handler)
            except:
                pass
            self._connected = False
            self.safe_connect()
        elif state == zookeeper.CONNECTED_STATE:
            utils.log(utils.cur(), self._name, event.state_name)
            self._connected = True
        else:
            utils.log(utils.cur(), self._name, event.state_name)
        self._conn_cv.notifyAll()
        self._conn_cv.release()

    def register_service(self, service, port=0):
        path = ""
        if service == "master":
            path = ZKConf.ZK_PATH_TEMP_MASTER
        elif service == "stats":
            path = ZKConf.ZK_PATH_TEMP_STATS
        elif service == "proxy":
            path = ZKConf.ZK_PATH_TEMP_PROXY
        
        if not path:
            raise Exception("service type '%s' doesn't exist" % (service,))

        node = zookeeper.exists(self._handler, path, None)
        if not node:
            if not zookeeper.exists(self._handler, ZKConf.ZK_PATH_ROOT, None):
                zookeeper.create(self._handler, ZKConf.ZK_PATH_ROOT, "", [ZKConf.ZOO_CREATOR_ALL_ACL], 0)
            zookeeper.create(self._handler, path, "", [ZKConf.ZOO_CREATOR_ALL_ACL], 0)

        ip = utils.getip()
        mepath = "".join([path, "/", ip, ":", str(port), "-"])
        if not port:
            mepath = "".join([path, "/", ip, "-"])

        childs = zookeeper.get_children(self._handler, path, None)
        is_created = False
        pathme = mepath.split("/")[-1]
        for child in childs:
            if child.startswith(pathme):
                is_created = child.split("-")[-1]
                break

        if is_created:
            meflag = mepath + is_created
            utils.log(utils.cur(), "%s none-callback %s" % ("*"*10, "*"*10), meflag)
        else:
            utils.log(utils.cur(), "%s %s %s" % ("*"*10, self.register_node_cb, "*"*10), mepath)
            meflag = zookeeper.create(self._handler,
                                      mepath,
                                      "",
                                      [ZKConf.ZOO_CREATOR_ALL_ACL],
                                      zookeeper.SEQUENCE|zookeeper.EPHEMERAL)
            utils.log(utils.cur(), "-"*10, meflag)

class ZooKeeper:
    def __init__(self, name, register_watch_cb=None, register_node_cb=None, *args, **kwargs):
        self._name = name
        self.register_watch_cb = register_watch_cb
        self.register_node_cb = register_node_cb
        self._args = args
        self._kwargs = kwargs
        self.ip = utils.getip()
        self._handler = None
        self.connect(True)
        self.i = 0

    def connect(self, first=False):
        try:
            if self._handler:
                zookeeper.close(self._handler)
        except:
            pass
        self._handler = Connection(self._name, self.register_watch_cb, first, self.register_node_cb, *self._args, **self._kwargs).handler

    def __nonzero__(self):
        return int(self.client_id()[0])

    def __str__(self):
        return str(self.client_id())

    def _wrap_watcher(self, watcher):
        if watcher is None:
            return watcher
        def wrapper(handle, event_type, conn_state, path):
            event = ClientEvent(event_type, conn_state, path)
            return watcher(event)
        return wrapper

    def __getattr__(self, name):
        if not isinstance(name, str) or not hasattr(zookeeper, name):
            raise ZooKeeperError("Method %s() doesn't exist" % str(name))

        if name in ("ASSOCIATING_STATE","AUTH_FAILED_STATE",
                    "CONNECTED_STATE","CONNECTING_STATE",
                    "EXPIRED_SESSION_STATE","NOTWATCHING_EVENT",
                    "SESSION_EVENT","CREATED_EVENT",
                    "DELETED_EVENT","CHANGED_EVENT","CHILD_EVENT"):
            return getattr(zookeeper, name)

        def safe_get(*args, **kwargs):
            func, result = getattr(zookeeper, name), None
            def real_func():
                if name in ("get", "exists", "get_children"):
                    path, watcher = args[0], args[1] if len(args) > 1 else None
                    return func(self._handler, path, self._wrap_watcher(watcher))
                else:
                    return func(self._handler, *args, **kwargs)
            try:
                result = real_func()
            except zookeeper.SessionExpiredException, err:
                utils.err(utils.cur(), err, "session expired, retry %s(%s,%s)" % (name, args, kwargs))
                self.connect()
                result = real_func()
            except zookeeper.ConnectionLossException, err:
                utils.err(utils.cur(), err, "connection loss, retry %s(%s,%s)" % (name, args, kwargs))
                self.connect()
                result = real_func()
            except zookeeper.OperationTimeoutException, err:
                utils.err(utils.cur(), err, "operation timeout, retry %s(%s,%s)" % (name, args, kwargs))
                self.connect()
                result = real_func()
            except SystemError, err:
                utils.err(utils.cur(), err, "system error, retry %s(%s,%s)" % (name, args, kwargs))
                self.connect()
                result = real_func()
            except zookeeper.NodeExistsException, err:
                utils.err(utils.cur(), err, "node exists, retry %s(%s,%s)" % (name, args, kwargs))
                raise zookeeper.NodeExistsException(err)
            except zookeeper.NoNodeException, err:
                utils.err(utils.cur(), err, "no node, retry %s(%s,%s)" % (name, args, kwargs))
                raise zookeeper.NoNodeException(err)
            except Exception, err:
                utils.err(utils.cur(), "Exception, retry %s(%s,%s)" % (name, args, kwargs), err, err.__class__)
            finally:
                return result

        return safe_get

    def watch_child(self, true_path, watcher):
        def _f_(event):
            if event.state_name == "connected" and event.type_name == "child":
                utils.log(utils.cur(), true_path, event, watcher.__name__ if callable(watcher) else watcher)
                self.watch_child(true_path, watcher)
                if callable(watcher):
                    try:
                        watcher(event, true_path)
                    except Exception, err:
                        utils.err(utils.cur(), err, err.__class__)
        try:
            self.get_children(true_path, _f_)
        except Exception, err:
            utils.err(utils.cur(), err)

    def watch_node(self, true_path, watcher):
        def _f_(event):
            if event.state_name == "connected" and event.type_name == "changed":
                utils.log(utils.cur(), true_path, event, watcher.__name__ if callable(watcher) else watcher)
                self.watch_node(true_path, watcher)
                if callable(watcher):
                    try:
                        watcher(event, true_path)
                    except Exception, err:
                        utils.err(utils.cur(), err, err.__class__)
        try:
            self.exists(true_path, _f_)
        except Exception, err:
            utils.err(utils.cur(), err)

    # def watch_node(self, true_path, watcher, is_child_watch=False):
    #     def _f_(event):
    #         if event.state_name == "connected" and (event.type_name == "child" if is_child_watch else "changed"):
    #             utils.log(utils.cur(), true_path, event, watcher.__name__ if callable(watcher) else watcher)
    #             utils.log(utils.cur(), "--real trigger--", self.i)
    #             self.i += 1
    #             self.watch_node(true_path, watcher, is_child_watch)
    #             if callable(watcher):
    #                 try:
    #                     watcher(event, true_path)
    #                 except Exception, err:
    #                     utils.err(utils.cur(), err, err.__class__)
    #     try:
    #         if is_child_watch:
    #             self.get_children(true_path, _f_)
    #         else:
    #             self.exists(true_path, _f_)
    #     except Exception, err:
    #         utils.err(utils.cur(), err)

    """
    create nodes recursively, like mkdir dirpath -p
    parameters are the same as function create()
    """
    def mknode(self, path, data="", acl=[ZKConf.ZOO_CREATOR_ALL_ACL], flags=0):
        path = path.strip()
        if not len(path) or path == "/" or not path.startswith("/"): return False
        parts = path.split('/')
        if parts[-1] == "": del parts[-1]
        if len(parts) < 2: return False
        
        k = 0
        length = len(parts)
        for part in parts:
            if k > 0 and k < length-1:
                mepath = "/".join(parts[:k+1])
                # check
                if not self.exists(mepath, None): 
                    self.create(mepath, "", acl, flags)
                # check again
                if not self.exists(mepath, None): 
                    return False
            elif k == length-1 :
                mepath = "/".join(parts[:k+1])
                # check
                if not self.exists(mepath, None): 
                    self.create(mepath, str(data), acl, flags)
                # check again
                if not self.exists(mepath, None): 
                    return False
            k += 1
        return True

    """
    delete nodes recursively, like rm dirpth -rf
    parameters are the same as function delete()
    """
    def delnode(self, path, version=-1):
        path = path.strip()
        if not self.exists(path, None): return False
        childs = self.get_children(path, None)
        if not childs:
            self.delete(path, version)
        else:
            for child in childs:
                self.delnode("".join([path, "/", child]), version)
            self.delete(path, version)
        return True
    
    def erase_forbid(self, db):
        self.delete(os.path.join(ZKConf.ZK_PATH_FORBID, db))
        
    def list(self, root="/", indent=0, mkdir=False):
        def make_path(child):
            if root == "/":
                return "/" + child
            return "".join([root, "/", child])
        
        childs = self.get_children(root, None)
        out = ""
        for i in xrange(indent):
            # out += "\t"
            out += "  "
        out = "".join(["|", out, "|--", os.path.basename(root), " :: ", self.get(root, None)[0]])
        print out
        if mkdir:
            os.system("mkdir -p %s" % root)
        # utils.log(utils.cur(), out)
        for child in childs:
            self.list(make_path(child), indent + 1, mkdir)

    def get_dict(self, root="/database"):
        tmp = {}
        def make_path(child):
            if root == "/":
                return "/" + child
            return "".join([root, "/", child])
        
        childs = self.get_children(root, None)
        tmp["__v__"] = self.get(root, None)[0]
        for child in childs:
            tmp[child] = self.get_dict(make_path(child))
        return tmp

    def trim_ips(self, ips):
        assert type(ips) in (str, list)
        if type(ips) is str:
            ips = ips.split("-")[0]
            ips = ips.split(":")[0]
        else:
            ips = [x.split("-")[0] for x in ips]
            ips = [x.split(":")[0] for x in ips]
        return ips

    def try_lock(self, data, path=ZKConf.LEADER_LOCK):
        utils.log(utils.cur(), "trying '%s'" % data)
        if self.exists(path, None):
            try:
                olddata, meta = self.get(path, None)
                utils.log(utils.cur(), 
                          "'%s' gets lock %s %s" % (data, 
                                                    olddata, 
                                                    olddata==data))
                return olddata == data
            except Exception, err:
                utils.err(utils.cur(), err)
            return False
        try:
            self.create(path, 
                        str(data), 
                        [ZKConf.ZOO_CREATOR_ALL_ACL], 
                        zookeeper.EPHEMERAL)
            utils.log(utils.cur(), "'%s' gets lock" % data)
            return True
        except Exception, err:
            utils.err(utils.cur(), err)
        return False

    def unlock(self, data, path=ZKConf.LEADER_LOCK):
        if not self.exists(path, None):
            utils.log(utils.cur(), 
                      "somebody else releases lock not me '%s'" % data)
            return True
        try:
            value = self.get(path, None)
            # "'NoneType' object is not iterable" means "zookeeper.get is None"
            if value is None:
                return True
            if value[0] == data:
                self.delete(path)
                utils.log(utils.cur(), "'%s' releases lock" % data)
                return True
        except Exception, err:
            utils.err(utils.cur(), err)
        return False

    def get_leaders(self, leader_path, specific_leader_path):
        childs = self.get_children(leader_path, None)
        ip_dict = dict()

        ip_self = self.trim_ips(self.get_children(specific_leader_path, None))
        ip_self = sorted(ip_self)[0] if len(ip_self) else ""
        
        for child in childs:
            child_path = os.path.join(leader_path, child)
            child_ips = self.get_children(child_path, None)
            child_ips = self.trim_ips(child_ips)
            for ip in child_ips:
                ip_dict[ip] = 1 if ip not in ip_dict else ip_dict[ip]+1
        return (ip_dict, ip_self)


    def is_proxy_master(self):
        proxy = self.get_children(ZKConf.ZK_PATH_TEMP_PROXY, None)
        ips = sorted([i.split(":")[0] for i in proxy]) if proxy else []
        return ips and self.ip == ips[0]

    def elect_leader(self, service_path, port, callback, specific_leader_path, first=False):
        meflag = None

        if not self.exists(specific_leader_path, None):
            self.mknode(specific_leader_path)

        def unsafe_check(event, check_callback, my_id):
            ip_dict, ip_self = self.get_leaders(ZKConf.ZK_PATH_LEADER, specific_leader_path)
            me_path = self.ip + ":" + str(port) if port else self.ip
            me_path = os.path.join(specific_leader_path, me_path)
            utils.log(utils.cur(), ip_dict, ip_self)
            utils.log(utils.cur(), self.ip, me_path)
            if ip_self:
                if (not self.ip in ip_dict) and \
                        (ip_self in ip_dict) and \
                        (ip_dict[ip_self] > 1):
                    leader_childs = self.get_children(specific_leader_path, None)
                    for leader_child in leader_childs:
                        self.delnode(os.path.join(specific_leader_path, leader_child))
                    utils.log(utils.cur(), me_path)
                    self.create(me_path, 
                                "", 
                                [ZKConf.ZOO_CREATOR_ALL_ACL], 
                                zookeeper.EPHEMERAL)
                    childs = sorted(self.get_children(specific_leader_path, None))
                    for child in childs[1:]:
                        self.delnode(os.path.join(specific_leader_path, child))
                    check_callback(my_id)
                    if len(childs) and childs[0].startswith(me_path.split("/")[-1]):
                        callback(True)
                    else:
                        callback(False)
                else:
                    utils.log(utils.cur(), my_id, ip_self)
                    childs = sorted(self.get_children(specific_leader_path, None))
                    for child in childs[1:]:
                        self.delnode(os.path.join(specific_leader_path, child))
                    check_callback(my_id)
                    if len(childs) and childs[0].startswith(my_id.split(":")[0]):
                        callback(True)
                    else:
                        callback(False)
            else:
                self.create(me_path, 
                            "", 
                            [ZKConf.ZOO_CREATOR_ALL_ACL], 
                            zookeeper.EPHEMERAL)
                utils.log(utils.cur(), me_path)
                childs = sorted(self.get_children(specific_leader_path, None))
                for child in childs[1:]:
                    self.delnode(os.path.join(specific_leader_path, child))
                check_callback(my_id)
                if len(childs) and childs[0].startswith(me_path.split("/")[-1]):
                    callback(True)
                else:
                    callback(False)

        def check_childs(event, true_path):
            try:
                utils.log(utils.cur(), event.type_name, event.state_name)
            except:
                utils.log(utils.cur(), event)
            my_id = self.ip + ":" + str(port) if port else self.ip
            i = 1
            while not self.try_lock(my_id) and i < 100:
                utils.log(utils.cur(), "'%s' tries lock %d times" % (my_id, i))
                i += 1
                time.sleep(1)
            utils.log(utils.cur(), "'%s' tries lock %d times and exit" % (my_id, i))
            if i < 100:
                utils.log(utils.cur(), "'%s' got lock and unsafe_checking" % my_id)
                unsafe_check(event, self.unlock, my_id)

        if not self.exists(service_path, None):
            self.create(service_path, "", [ZKConf.ZOO_CREATOR_ALL_ACL], 0)

        mepath = "".join([service_path, "/", self.ip, "-"])
        if port:
            mepath = "".join([service_path, "/", self.ip, ":", str(port), "-"])

        childs = self.get_children(service_path, None)
        self.watch_child(service_path, check_childs)
        utils.log(utils.cur(), service_path, childs)

        created = False
        pathme = mepath.split("/")[-1]
        for child in childs:
            if child.startswith(pathme):
                created = child.split("-")[-1]
                break

        if created:
            meflag = mepath + created
            check_childs(None, None)
        else:
            meflag = self.create(mepath, 
                                 "", 
                                 [ZKConf.ZOO_CREATOR_ALL_ACL], 
                                 zookeeper.SEQUENCE|zookeeper.EPHEMERAL)
