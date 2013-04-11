#!/usr/bin/env python
#-*- coding:utf-8 -*-
######################################################################
## Filename:      mc_helper.py
##                
## Copyright (C) 2012,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Wed May 23 14:41:58 2012
##                
## Description:   
##                
######################################################################
import sys, memcache

import utils
from constants import MCConf

class Memcached(object):
    def __init__(self):
        self.mc = memcache.Client(MCConf.HOST)
        self._prefix = MCConf.PREFIX

    def close(self):
        self.mc.disconnect_all()

    def mc_key(self, key):
        return self._prefix + str(key)

    def get(self, key):
        return self.mc.get(self.mc_key(key))
    def gets(self, key):
        return self.mc.gets(self.mc_key(key))

    def set(self, key, value):
        return self.mc.set(self.mc_key(key), value)

    def incr(self, key, delta=1):
        return self.mc.incr(self.mc_key(key), delta)

    def decr(self, key, delta=1):
        return self.mc.decr(self.mc_key(key), delta)

