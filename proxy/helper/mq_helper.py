#!/usr/bin/env python
#-*- coding: utf-8 -*-
######################################################################
## Filename:      mq_helper.py
##                
## Copyright (C) 2012,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Fri Feb  3 15:30:21 2012
##                
## Description:   
##                
######################################################################
import sys, time, pika, threading
from pika.exceptions import *

import utils
from constants import MQConf

def deco_check_channel(func):
    def deco(obj, *args, **kwargs):
        obj.try_connect()
        return func(obj, *args, **kwargs)
    return deco

class Pika(object):
    def __init__(self, servers=[], exchange="", routing_key="", type="fanout"):
        self.params = PikaParams(servers)
        self.exchange = exchange
        self.routing_key = routing_key
        self.exchange_type = type
        self.connection = None
        self.channel = None
        self.try_times = MQConf.MAX_RETRY_TIMES
        self.try_index = 0
        self.running = True

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.disconnect()
            self.connection = None
            self.channel = None
        self.running = False

    def try_connect(self):
        if self.connection and not self.connection.is_open:
            self.connection.disconnect()
            self.connection = None
            self.channel = None
        self.try_index = 0
        while self.running and not self.connection and self.try_index < self.try_times:
            try:
                self.connection = pika.BlockingConnection(self.params.get_param())
            except:
                pass
            self.try_index += 1
        if self.running and not self.connection:
            raise AttributeError("All MQ servers are down")
        if self.running and not self.channel:
            self.channel = self.connection.channel()

    @deco_check_channel
    def send_msg(self, msg, routing_key=""):
        def do_send():
            try:
                self.channel.exchange_declare(exchange=self.exchange, type=self.exchange_type)
                self.channel.basic_publish(exchange=self.exchange,
                                           routing_key=routing_key or self.routing_key,
                                           body=msg,
                                           properties=pika.BasicProperties(delivery_mode=2)) # make message persistent
            except AMQPChannelError, err:
                self.try_connect()
                if self.running: do_send()
            except Exception, err:
                print err
                self.try_connect()
                if self.running: do_send()
        do_send()

    @deco_check_channel
    def recv_msg(self, callback, queue, routing_key=""):
        def do_recv():
            try:
                self.channel.exchange_declare(exchange=self.exchange, type=self.exchange_type)
                self.channel.queue_declare(queue=queue, 
                                           durable=True, # make queue durable
                                           arguments={
                        "x-ha-policy": "all", # Mirrored Queues
                        "x-message-ttl": MQConf.MESSAGE_TTL}) # message Time-To-Live, i.e. message expiry time
                self.channel.queue_bind(exchange=self.exchange, 
                                        queue=queue, 
                                        routing_key=routing_key or self.routing_key)
                self.channel.basic_consume(callback, queue=queue)
                self.channel.start_consuming()
            except AMQPChannelError, err:
                self.try_connect()
                if self.running: do_recv()
            except Exception, err:
                print err
                self.try_connect()
                if self.running: do_recv()
        do_recv()

class PikaRecvThread(threading.Thread):
    def __init__(self, servers, exchange="", queue="", callback=None):
        threading.Thread.__init__(self)
        self.pika = Pika(servers=servers, exchange = exchange)
        self.queue = queue
        self.callback = callback
        self.running = True
        self.is_server_down = False

    def run(self):
        while self.running:
            if callable(self.callback):
                try:
                    self.pika.recv_msg(self.callback, self.queue)
                except Exception, err:
                    utils.err(utils.cur(), err)
                    self.is_server_down = True
                    self.try_reconnect()

    def try_reconnect(self):
        while self.running and self.is_server_down:
            try:
                self.pika.try_connect()
                self.is_server_down = False
            except Exception, err:
                utils.err(utils.cur(), time.time(), err)
                time.sleep(MQConf.RECONNECTION_TIME)

    def stop(self):
        if self.isAlive():
            self.running = False
            self.pika.close()

class PikaSendThread(threading.Thread):
    def __init__(self, servers, exchange=""):
        threading.Thread.__init__(self)
        self.pika = Pika(servers=servers, exchange = exchange)
        self.running = True
        self.is_server_down = False

    def run(self):
        pass

    def send_msg(self, msg):
        try:
            self.pika.send_msg(msg)
        except Exception, err:
            utils.err(utils.cur(), err)
            self.is_server_down = True
            self.try_reconnect()

    def try_reconnect(self):
        while self.running and self.is_server_down:
            try:
                self.pika.try_connect()
                self.is_server_down = False
            except Exception, err:
                utils.err(utils.cur(), time.time(), err)
                time.sleep(MQConf.RECONNECTION_TIME)

    def stop(self):
        if self.isAlive():
            self.running = False
            self.pika.close()
    
class PikaParams(object):
    def __init__(self, servers):
        self.host_key = "host"
        self.port_key = "port"
        self.servers = list()
        self.check_servers(servers)
        self.srv_index = 0
        self.credentials = pika.PlainCredentials(MQConf.USERNAME, MQConf.PASSWORD)

    def get_param(self):
        if self.srv_index == len(self.servers):
            self.srv_index = 0
        host = self.servers[self.srv_index][self.host_key]
        port = self.servers[self.srv_index][self.port_key]
        self.srv_index += 1
        return pika.ConnectionParameters(host=host, 
                                         port=port, 
                                         credentials=self.credentials)

    def check_servers(self, servers):
        if not isinstance(servers, list):
            raise AttributeError("Parameter '%s' MUST be a list" % self.must_key)
        if not len(servers):
            raise AttributeError("Parameter '%s' MUST NOT NULL" % self.must_key)
        for srv in servers:
            if not isinstance(srv, dict):
                raise AttributeError("Server info MUST be a dict")
            if not self.host_key in srv:
                raise AttributeError("Server info MUST contain '%s'" % self.host_key)
            if not isinstance(srv[self.host_key], str):
                raise AttributeError("'%s' info MUST a str" % self.host_key)
            if not self.port_key in srv:
                raise AttributeError("Server info MUST contain '%s" % self.port_key)
            if not isinstance(srv[self.port_key], (int, long)):
                raise AttributeError("'%s' info MUST either an int or a long" % self.port_key)
        self.servers = servers

