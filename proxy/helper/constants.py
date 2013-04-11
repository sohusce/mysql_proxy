#!/usr/bin/env python
# -*- coding: utf-8 -*-
######################################################################
## Filename:      constants.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Sun Apr  1 11:50:24 2012
##                
## Description:   It's not a joke.
##                
######################################################################

import sys, random, __builtin__, Queue, urllib2
from collections import namedtuple

import utils

class _constants(object):
    APPNAME = 'app'
    PORT_MIN = 61050
    PORT_MAX = 61099
    def __new__(cls):
        raise TypeError, "Can not instanciate from %s" % cls.__name__

class Version(_constants):
    LOCAL = 0
    DEV = 1
    TEST = 2
    RELEASE = 3
    VERSIONS = {"LOCAL":LOCAL, 
                "SERVER_LOCAL":LOCAL, 
                "DEV":DEV, 
                "TEST":TEST, 
                "RELEASE":RELEASE}

VERSION = Version.LOCAL
# timeout for client conection
WAIT_TIMEOUT = 300 # seconds  5 minutes

class ProxyServer(_constants):
    PORT = _constants.PORT_MIN
    PIDFILE = "/opt/conf/sce_db/sce_db_proxy.pid"
    LOGFILE = "/opt/logs/sce_db/sce_db_proxy.log"
    ERRFILE = "/opt/logs/sce_db/sce_db_proxy_error.log"
    LOGTWISTD = "/opt/logs/sce_db/twistd_sce_db_proxy.log"
    LOGGER  = "sce_db_proxy_logger"
    APPLICATION = "sce-proxy"

    POOL_MIN = 0                # min_connections
    POOL_MAX = 400              # max_connections
    POOL_IDLE = 300             # idle_time seconds

    CONN_TIMEOUT = 5
    SERVERS = []

class MCConf(_constants):
    HOST = ["10.10.10.10:11211"]
    PREFIX = "dbproxy_"

class Cipher(_constants):
    OFB = 0
    CFB = 1
    CBC = 2

    SIZE_128 = 16
    SIZE_192 = 24
    SIZE_256 = 32

    SALT = "saltsalt"
    IV = [103,35,148,239,76,213,47,118,255,222,123,176,106,134,98,92]

# for apps exeeding quotas
class Forbid(_constants):
    TEMP_MAXCONNS = 20

    # type
    FORBID_ABORTION  = 0  # normal
    FORBID_WORKING   = 1  # forbid temporarily
    FORBID_FOREVER   = 2  # forbid permanently

    # object
    FORBID_DATABASE  = 10
    FORBID_TABLE     = 11

    # duration
    DURATION_DEFAULT = 300 # seconds
    DURATION_FIVEMIN = 300 # seconds
    DURATION_ONEHOUR = 3600 # seonds

    START_NONE = 0

    # crud
    OPERATION_DEFAULT = "ALL"
    OPERATION_INSERT = "INSERTE"
    OPERATION_SELECT = "SELECT"
    OPERATION_UPDATE = "UPDATE"
    OPERATION_DELETE = "DELETE"

    KEY_TYPE = "type"
    KEY_START = "start"
    KEY_DURATION = "duration"
    KEY_OBJECT = "object"
    KEY_CRUD = "crud"
    # crud
    # ["update", "select"]
    # {tbl1 : ["insert"], tbl2 : ["delete"]}
    FORBID_MORE = {
        "type": FORBID_ABORTION,
        "start" : START_NONE,
        "duration": DURATION_DEFAULT,
        "object" : "",
        "crud": [],
        "errmsg": ""
        }

    FORBID_LESS = {
        "type": FORBID_WORKING,
        "start" : START_NONE,
        "duration": DURATION_DEFAULT,
        "object" : FORBID_DATABASE,
        "crud": [OPERATION_DEFAULT],
        "errmsg": "quota exceeded"
        }

# for RabbitMQ
class MQConf(_constants):
    MESSAGE_TTL = 60 * 60 * 1000 # 1 hour to milliseconds
    USERNAME = "sce_username"
    PASSWORD = "sce_password"
    MAX_RETRY_TIMES = 3

    MQ_EXCHANGE = "test"
    EXCHANGE_FORBID = "forbid"
    PROXY_SUFFIX = "_forbid"
    RECONNECTION_TIME = 60

    SERVERS = [{"host" : "10.10.10.10", "port" : 5673}, 
               {"host" : "10.10.10.11", "port" : 5673}]

# for SQL verification
class SQLState(_constants):
    ENGINE_SUPPORTTING = ["INNODB"]
    SQL_NORMAL = 0
    SQL_ENGINE = 1
    SQL_PRIVATE = 2

class AuthConf(_constants):
    PACKET_TYPES = {
        "LENENC_INT" : 1,
        "LENENC_NULL": 2,
        "LENENC_EOF" : 3,
        "LENENC_ERR" : 4
        }

    OK_STATUS  = 0x00
    EOF_STATUS = 0xFE
    ERR_STATUS = 0xFF

    MASK_UNSIGNED_LONG = sys.maxint
    MASK_SCRAMBLE_323  = (1 << 31) - 1

    SCRAMBLE_LENGTH_323 = 8
    MAX_PACKET_LENGTH   = 0xffffff

    SERVER_STATUS_IN_TRANS = 0x0001
    SERVER_STATUS_AUTOCOMMIT = 0x0002
    SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010
    SERVER_STATUS_NO_INDEX_USED = 0x0020

class ServerCapability(_constants):
    CLIENT_LONG_PASSWORD     = 1      # /* new more secure passwords */
    CLIENT_FOUND_ROWS	     = 2      # /* Found instead of affected rows */
    CLIENT_LONG_FLAG	     = 4      # /* Get all column flags */
    CLIENT_CONNECT_WITH_DB   = 8      # /* One can specify db on connect */
    CLIENT_NO_SCHEMA	     = 16     # /* Dont allow database.table.column */
    CLIENT_COMPRESS          = 32     # /* Can use compression protocol */
    CLIENT_ODBC	             = 64     # /* Odbc client */
    CLIENT_LOCAL_FILES       = 128    # /* Can use LOAD DATA LOCAL */
    CLIENT_IGNORE_SPACE	     = 256    # /* Ignore spaces before ( */
    CLIENT_PROTOCOL_41	     = 512    # /* New 4.1 protocol */
    CLIENT_INTERACTIVE	     = 1024   # /* This is an interactive client */
    CLIENT_SSL               = 2048   # /* Switch to SSL after handshake */
    CLIENT_IGNORE_SIGPIPE    = 4096   # /* IGNORE sigpipes */
    CLIENT_TRANSACTIONS      = 8192   # /* Client knows about transactions */
    CLIENT_RESERVED          = 16384  # /* Old flag for 4.1 protocol  */
    CLIENT_SECURE_CONNECTION = 32768  # /* New 4.1 authentication */
    CLIENT_MULTI_STATEMENTS  = 65536  # /* Enable/disable multi-stmt support */
    CLIENT_MULTI_RESULTS     = 131072 # /* Enable/disable multi-results */

class MyState(_constants):
    MY_UNKNOWN          = 0x00
    MY_INIT             = 0x01
    MY_RECV_HAND        = 0x02
    MY_SEND_AUTH        = 0x03
    MY_RECV_AUTH_RESULT = 0x04
    MY_AUTHED           = 0x05
    MY_UNAUTHED         = 0x06
    MY_SEND_AUTH_SECURE        = 0x07
    MY_RECV_AUTH_SECURE_RESULT = 0x08

class Command(_constants):
    COM_SLEEP           = 0x00 # (none this is an internal thread state)
    COM_QUIT            = 0x01 # mysql_close
    COM_INIT_DB         = 0x02 # mysql_select_db 
    COM_QUERY           = 0x03 # mysql_real_query
    COM_FIELD_LIST      = 0x04 # mysql_list_fields
    COM_CREATE_DB       = 0x05 # mysql_create_db (deprecated)
    COM_DROP_DB         = 0x06 # mysql_drop_db (deprecated)
    COM_REFRESH         = 0x07 # mysql_refresh
    COM_SHUTDOWN        = 0x08 # mysql_shutdown
    COM_STATISTICS      = 0x09 # mysql_stat
    COM_PROCESS_INFO    = 0x0a # mysql_list_processes
    COM_CONNECT         = 0x0b # (none this is an internal thread state)
    COM_PROCESS_KILL    = 0x0c # mysql_kill
    COM_DEBUG           = 0x0d # mysql_dump_debug_info
    COM_PING            = 0x0e # mysql_ping
    COM_TIME            = 0x0f # (none this is an internal thread state)
    COM_DELAYED_INSERT  = 0x10 # (none this is an internal thread state)
    COM_CHANGE_USER     = 0x11 # mysql_change_user
    COM_BINLOG_DUMP     = 0x12 # sent by the slave IO thread to request a binlog
    COM_TABLE_DUMP      = 0x13 # LOAD TABLE ... FROM MASTER (deprecated)
    COM_CONNECT_OUT     = 0x14 # (none this is an internal thread state)
    COM_REGISTER_SLAVE  = 0x15 # sent by the slave to register with the master (optional)
    COM_STMT_PREPARE    = 0x16 # mysql_stmt_prepare
    COM_STMT_EXECUTE    = 0x17 # mysql_stmt_execute
    COM_STMT_SEND_LONG_DATA = 0x18 # mysql_stmt_send_long_data
    COM_STMT_CLOSE      = 0x19 # mysql_stmt_close
    COM_STMT_RESET      = 0x1a # mysql_stmt_reset
    COM_SET_OPTION      = 0x1b # mysql_set_server_option
    COM_STMT_FETCH      = 0x1c # mysql_stmt_fetch

class DBConf(_constants):
    TIME_INTERVAL = 60 # seconds

    TABLE_EVENT  = "t_event"
    TABLE_DBINFO = "t_db_info"
    TABLE_DBTYPE = "t_db_type"

    TABLE_STAT = "t_db_proxy_stat"
    TABLE_PERF = "t_db_performance"

    APPENGINEDB = {
        "host" : "10.10.10.10",
        "port" : 3306,
        "user" : "username",
        "passwd" : "password",
        "db" : "dbname",
        "charset" : "utf8"
        }
    COMMONDB = {
        "host" : "10.10.10.11",
        "port" : 3306,
        "user" : "username",
        "passwd" : "password",
        "db" : "dbname",
        "charset" : "utf8"
        }

    COMMONDB_UNICODE = True
    COMMONDB_CHARSET = "utf8"
    COMMONDB_MAXCONNS = 1000
    COMMONDB_MINCACHE = 1
    COMMONDB_MAXCACHE = 10
    COMMONDB_BLOCKING = False

    SYNCMCDB = {
        "host" : "10.10.10.10",
        "port" : 61050,
        "user" : "username",
        "passwd" : "password",
        "db" : "dbname",
        "charset" : "utf8"
        }

class ZKConf(_constants):
    LEADER_LOCK = "/database/leader_lock"
    ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"}
    ZOO_CREATOR_ALL_ACL = {"perms":0x1f, "scheme":"auth", "id" :""}
    ZOO_SESSION_TIMEOUT = 5000  # milliseconds
    ZOO_USERNAME = "sce-username"
    ZOO_PASSWORD = "sce-password"

    ZK_HOST = "10.10.10.10:2181,10.10.10.11:2181,10.10.10.12:2181"

    ZK_PATH_ROOT = "/database"
    ZK_PATH_LEADER_DB_STATS  = "/database/leader/db_stats"
    ZK_PATH_LEADER_DB_MASTER = "/database/leader/db_master"
    ZK_PATH_LEADER = "/database/leader"

    ZK_PATH_TEMP_MASTER = '/database/temp_master'
    ZK_PATH_TEMP_STATS  = '/database/temp_stats'
    ZK_PATH_TEMP_PROXY  = "/database/temp_proxy"

    ZK_PATH_FORBID = "/database/forbid"
    ZK_PATH_DB = "/database/db_info"
    ZK_PATH_IPS = "/database/authed_ips"
    ZK_PATH_TYPE = "/database/db_type"
    KEY_READ = "db_host_r"
    KEY_WRITE = "db_host_w"
    KEY_PASSWORD = "db_password"
    KEY_USER = "db_user"
    KEY_DB = "db_db"
    KEY_IP = "authed_ips"
    KEY_DBCONF = "dbconf"
    KEY_DBTYPE = "dbtype"

    KEY_DISABLED = "db_disabled"
    DISABLED = 1

    INTERNAL = "internal"

class ErrorCode(_constants):
    CONNECT_ERR = {
        "errno" : 2901,
        "sqlstate" : "#SCE01",
        "message"  : "connect server failed, please check username, password and database "
        }

    DB_NOT_EXISTED = {
        "errno" : 2902,
        "sqlstate" : "#SCE02",
        "message"  : "database is not existed"
        }

    PACKET_WRONG = {
        "errno" : 2903,
        "sqlstate" : "#SCE03",
        "message"  : "Auth packet must contain database"
        }

    AUTH_WRONG = {
        "errno" : 2904,
        "sqlstate" : "#SCE04",
        # eg. "Access denied for user "webim"@"10.10.10.10" (using password: YES)"
        "message"  : "Access denied for user '%s'@'%s' (using password: %s)"
        }

    USE_FORBIDDEN = {
        "errno" : 2905,
        "sqlstate" : "#SCE05",
        "message"  : "Database is forbidden"
        }

    SQL_FORBIDDEN = {
        "errno" : 2906,
        "sqlstate" : "#SCE06",
        "message"  : "SQL statement is forbidden"
        }

    ENGINE_NOT_SUPPORT = {
        "errno" : 2907,
        "sqlstate" : "#SCE07",
        "message"  : "Engine '%s' isn't being supported"
        }

    ENGINE_NOT_SPECIFIED = {
        "errno" : 2908,
        "sqlstate" : "#SCE08",
        "message"  : "Engine isn't specified."
        }

    GONE_AWAY = {
        "errno" : 2909,
        "sqlstate" : "#SCE09",
        "message"  : "MySQL server has gone away"
        }

    QUOTA_EXCEEDED = {
        "errno" : 2910,
        "sqlstate" : "#SCE10",
        "message"  : "%s operations in %s '%s' is still forbidden because of '%s'. Try again %s seconds later."
        }

    IP_RESTRICTED = {
        "errno" : 2911,
        "sqlstate" : "#SCE11",
        "message"  : "'%(ip)s' is not in my service. Please contact administrator."
        }

    DB_DISABLED = {
        "errno" : 2912,
        "sqlstate" : "#SCE12",
        "message"  : "'%(db)s' is disabled."
        }

    POOL_ERROR = {
        "errno" : 2913,
        "sqlstate" : "#SCE13",
        "message"  : "%s"
        }

    SERVICE_ERROR = {
        "errno" : 2914,
        "sqlstate" : "#SCE14",
        "message"  : "Cant't connect to MySQL server on '%s' (111)"
        }

    BACKEND_TIMEOUT = {
        "errno" : 2915,
        "sqlstate" : "#SCE15",
        "message"  : "Connect to Backend MySQL server timeout"
        }

class QuotaKeys(object):
    """
    {"maxconn": 1024, 
     "network": 8589934592, 
     "tablesize": 10, 
     "slowquery_duration": 10, 
     "slowquery_times": 100, 
     "tablerows": 10000, 
     "disk": 2147483648,
     "tablenums": 100}
    """
    # max number of tables
    DB_TABLE_TOTAL = "tablenums"
    # max number of table rows
    DB_TABLEROW_TOTAL = "tablerows"
    # max size of disk
    DB_DISKTOTAL = "disk"
    # max number of connections
    DB_CONN_TOTAL = "maxconn"
    # max table size
    DB_TABLESIZE_TOTAL = "tablesize"
    # max sum of slow queries
    DB_SLOWQUERYDURATION_TOTAL = "slowquery_duration"
    # max times of slow queries
    DB_SLOWQUERYTIMES_TOTAL = "slowquery_times"
    # max network traffic
    DB_NETTOTAL = "network"
    # max CPU time
    DB_CPUTIME = "cputime"

class Quota(_constants):
    # 2GB disk
    DISKTOTAL = 2 * 2**30
    # 1024 connections
    MAXCONN = 1024
    # 8GB network bandwidth
    NETTOTAL = 8 * 2**30
    # 100 thousand rows
    TABLEROWS = 10 * 10000
    # 100 tables
    TABLENUMS = 100
    # 1GB table size
    TABLESIZE = 2**30
    # 2 seconds
    SQ_DURATION = 2
    # 5 times
    SQ_TIMES = 5
    # 60 seconds
    CPUTIME = 60

    PUBLIC_TYPE = 0
    PRIVATE_TYPE = 1
    DISABLED = 1

    Keys = {
        QuotaKeys.DB_DISKTOTAL: DISKTOTAL,
        QuotaKeys.DB_CONN_TOTAL: MAXCONN,
        QuotaKeys.DB_NETTOTAL: NETTOTAL, 
        QuotaKeys.DB_TABLESIZE_TOTAL: TABLESIZE, 
        QuotaKeys.DB_TABLEROW_TOTAL: TABLEROWS, 
        QuotaKeys.DB_TABLE_TOTAL: TABLENUMS,
        QuotaKeys.DB_SLOWQUERYDURATION_TOTAL: SQ_DURATION, 
        QuotaKeys.DB_SLOWQUERYTIMES_TOTAL: SQ_TIMES, 
        QuotaKeys.DB_CPUTIME: CPUTIME,
        }

def change_config():
    __builtin__.stats_conns = StatsConns()
    __builtin__.proxy_stats = Queue.Queue() # log cache for proxy_stats
