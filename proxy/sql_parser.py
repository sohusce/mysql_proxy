#!/usr/bin/env python
#-*- coding: utf-8 -*-
######################################################################
## Filename:      sql_parser.py
##                
## Copyright (C) 2012,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Thu Aug 23 13:43:58 2012
##                
## Description:   
##                
######################################################################

import sce_token, sce_tokenize
from collections import namedtuple

from helper import utils
from mysql_packet import pack_dict_packet
from helper.constants import SQLState, ErrorCode, ZKConf


class SQLVerify:

    whitelists = {
        "SELECT"   : [SQLState.SQL_NORMAL, sce_token.TK_SQL_FROM],
        "UPDATE"   : [SQLState.SQL_NORMAL, sce_token.TK_SQL_UPDATE],
        "DELETE"   : [SQLState.SQL_NORMAL, sce_token.TK_SQL_FROM],
        "INSERT"   : [SQLState.SQL_NORMAL, sce_token.TK_SQL_INTO],
        "ALTER"    : [SQLState.SQL_NORMAL, sce_token.TK_SQL_TABLE, "TABLE"],
        "CREATE"   : (
            [SQLState.SQL_ENGINE, sce_token.TK_SQL_TABLE, "TABLE"],
            [SQLState.SQL_NORMAL, sce_token.TK_SQL_ON, "INDEX"],
            ),
        "DROP"     : [SQLState.SQL_NORMAL, sce_token.TK_SQL_TABLE, "TABLE"],
        "SHOW"     : (
            [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN, "TABLES"],
            [SQLState.SQL_NORMAL, sce_token.TK_SQL_TABLE, "CREATE"],
            ),
        "COMMIT"   : [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN],
        "ROLLBACK" : [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN],
        "START"    : [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN, "TRANSACTION"],
        "SET"      : [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN, "AUTOCOMMIT"],
        "SCE"      : [SQLState.SQL_PRIVATE, sce_token.TK_UNKNOWN],
        }

    internal_wls = {
        "SHOW"     : (
            [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN, "PROCESSLIST"],
            [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN, "DATABASES"],
            [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN],
            ),
        "DESC"     : [SQLState.SQL_NORMAL, sce_token.TK_SQL_DESC],
        "DESCRIBE" : [SQLState.SQL_NORMAL, sce_token.TK_SQL_DESCRIBE],
        "SET"      : [SQLState.SQL_NORMAL, sce_token.TK_UNKNOWN],
        }

    def __init__(self, sql=""):
        self.reset()
        self.sql = sql.strip()
        self.merged_wls = {}
        self.merge_wls()
        self.tuple = namedtuple("ReturnDict", "use_master, sql_state, msg, opts")

    def reset(self):
        self.tokens = []
        self.dbopts = []
        self.tbopts = {}

    def addSQL(self, sql):
        self.sql = sql.strip()
        self.reset()
        self.tokenize()
        return self.tokens

    def tokenize(self):
        tmp_tokens = sce_tokenize.tokenize(self.sql)
        self.tokens = list(tmp_tokens)
        # remove comments and semicolon
        for tk, tk_id in tmp_tokens:
            if tk_id in (sce_token.TK_COMMENT, sce_token.TK_SEMICOLON):
                self.tokens.remove((tk, tk_id))

    def merge_wls(self):
        wls = dict(self.whitelists)
        for k, wl in self.internal_wls.iteritems():
            if k not in wls:
                wls[k] = wl
            else:
                if not wl: continue
                if isinstance(wls[k], list):
                    wls[k] = (wls[k],)

                if isinstance(wl, list) and wl not in wls[k]:
                    wls[k] += (wl,)
                elif isinstance(wl, tuple):
                    for l in wl:
                        if l not in wls[k]:
                            wls[k] += (l,)
        self.merged_wls = dict(wls)
        del wls

    @property
    def opts(self):
        return {"db": self.dbopts, "tb": self.tbopts}

    def get_privs(self, whitelist):
        privs = []
        token_ids = [x[1] for x in self.tokens]
        crud = self.tokens[0][0]
        self.dbopts.append(crud)
        if isinstance(whitelist, list):
            tmp = self.check_whitelist(whitelist, crud, token_ids)
            if tmp: privs.append(tmp)
        else:
            for wl in whitelist:
                tmp = self.check_whitelist(wl, crud, token_ids)
                if tmp: privs.append(tmp)
        return privs

    def check_whitelist(self, wl, crud, token_ids):
        if len(self.tokens) < len(wl[2:])+1: return False
        i = 0
        for kw in wl[2:]:
            i += 1
            if self.tokens[i][0] != kw: return False
        tbl = None
        utils.log(utils.cur(), wl, wl[1], token_ids, self.tokens)
        if wl[1] != sce_token.TK_UNKNOWN:
            try:
                idx = token_ids.index(wl[1], i)
                utils.log(utils.cur(), wl, idx)
                if idx+1 < len(self.tokens) \
                        and self.tokens[idx+1][1] == sce_token.TK_LITERAL:
                    tbl = self.tokens[idx+1][0]
            except:
                pass
        if tbl:
            if tbl in self.tbopts:
                self.tbopts[tbl].append(crud)
            else:
                self.tbopts[tbl] = [crud]
        return (True, tbl, wl)

    def deal_sql_engine(self, tag):
        """
        `CREATE TABLE` only supports engine InnoDB
        """
        key_engine = "ENGINE"
        token_names = [x[0] for x in self.tokens]
        if key_engine in token_names:
            idx = token_names.index(key_engine)
            if idx > 0 and idx+2 < len(self.tokens) \
                    and self.tokens[idx+1][1] == sce_token.TK_EQ:
                if self.tokens[idx+2][0] not in SQLState.ENGINE_SUPPORTTING:
                    err_dict = dict(ErrorCode.ENGINE_NOT_SUPPORT)
                    err_dict["message"] = err_dict["message"] % self.tokens[idx+2][0]
                    return self.tuple("err", tag, err_dict, self.opts)
            else:
                err_dict = dict(ErrorCode.ENGINE_NOT_SPECIFIED)
                return self.tuple("err", tag, err_dict, self.opts)

    def deal_sql_private(self, tag, proxyconns, busyobj, idx):
        """
        sce pool/proxy/client status
        sce master = 0/1
        """
        sce_status = [("POOL", "PROXY", "CLIENT"), "STATUS"]
        sce_master = ["MASTER", "=", (0, 1)]
        tks = [x[0] for x in self.tokens]
        if len(tks) == 3 \
                and tks[1] in sce_status[0] \
                and tks[2] == sce_status[1]:
            if tks[1] == "POOL":
                ok_value = pack_dict_packet(stats_conns.pool(), 
                                            keys=["database", "connections"], 
                                            idx=idx)
            elif tks[1] == "PROXY":
                cached_conns = dict(proxyconns.show())
                active_conns = dict(stats_conns.show())
                common_dbs = set(cached_conns) & set(active_conns)
                common_conns = []
                for db in common_dbs:
                    common_conns.append((db, active_conns[db], cached_conns[db], 
                                         active_conns[db] + cached_conns[db]))
                ok_value = pack_dict_packet(common_conns, 
                                            keys=["database", "active_conns", "cached_conns", "all_conns"], 
                                            idx=idx)
            else:
                conns = []
                for db in busyobj:
                    conns.append((db, len(busyobj[db])))
                ok_value = pack_dict_packet(conns, 
                                            keys=["busyobj", "connections"],
                                            idx=idx)
            return self.tuple("ok", SQLState.SQL_PRIVATE, ok_value, self.opts)
        elif len(tks) == 4 \
                and tks[1] == sce_master[0] \
                and tks[2] == sce_master[1] \
                and self.tokens[3][1] == sce_token.TK_INTEGER \
                and int(tks[3]) in sce_master[2]:
            return self.tuple("ok", SQLState.SQL_PRIVATE, int(tks[3]), self.opts)
        else:
            err_dict = dict(ErrorCode.SQL_FORBIDDEN)
            return self.tuple("err", tag, err_dict, self.opts)

    def verify(self, dbtype, forceOk, proxyconns, busyobj, idx):
        tag = SQLState.SQL_NORMAL
        if self.sql and len(self.tokens) == 0:
            self.tokenize()

        if not len(self.tokens):
            err_dict = dict(ErrorCode.SQL_FORBIDDEN)
            return self.tuple("err", tag, err_dict, self.opts)

        use_master = False
        first_token, first_token_id = self.tokens[0]
        utils.log(utils.cur(), first_token, first_token_id, self.tokens)
        if first_token_id == sce_token.TK_SQL_SELECT:
            # when self.tokens[1:] is None, use_master is still False
            for token, token_id in self.tokens[1:]:
                # select last_insert_id(); or select row_count();
                if token_id == sce_token.TK_FUNCTION and token in ("LAST_INSERT_ID", "ROW_COUNT"):
                    use_master = True
                    break
                # select @@insert_id;
                elif token_id == sce_token.TK_LITERAL and token in ("@@INSERT_ID", "@@IDENTITY"):
                    use_master = True
                    break
                else:
                    use_master = False
        else:
            use_master = True

        if forceOk:
            return self.tuple(use_master, tag, "", self.opts)

        wls = self.merged_wls if dbtype == ZKConf.INTERNAL else self.whitelists
        if first_token not in wls:
            err_dict = dict(ErrorCode.SQL_FORBIDDEN)
            return self.tuple("err", tag, err_dict, self.opts)

        whitelist = wls[first_token]
        assert isinstance(whitelist, (list, tuple)), "whitelist must be a list or tuple"

        privs = self.get_privs(whitelist)
        if not privs:
            err_dict = dict(ErrorCode.SQL_FORBIDDEN)
            return self.tuple("err", tag, err_dict, self.opts)

        specials = [x for x in privs if not x[2][0] == SQLState.SQL_NORMAL]
        utils.log(utils.cur(), privs, specials)
        for priv in specials:
            sql_state = priv[2][0]
            if sql_state == SQLState.SQL_ENGINE:
                tmp = self.deal_sql_engine(tag)
                if tmp: return tmp
            elif sql_state == SQLState.SQL_PRIVATE:
                tmp = self.deal_sql_private(tag, proxyconns, busyobj, idx)
                if tmp: return tmp

        return self.tuple(use_master, tag, "", self.opts)
