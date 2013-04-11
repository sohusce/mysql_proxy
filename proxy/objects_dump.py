#!/usr/bin/env python
#-*- coding:utf-8 -*-
######################################################################
## Filename:      objects_dump.py
##                
## Copyright (C) 2012-2013,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Tue Jun 12 10:49:07 2012
##                
## Description:   
##                
######################################################################

from meliae import scanner, loader

class MemObject(object):

    @staticmethod
    def dump(filename):
        scanner.dump_all_objects(filename)

    @staticmethod
    def load(filename):
        om = loader.load(filename)
        om.compute_parents()
        om.collapse_instance_dicts()
        print om.summarize()

    @staticmethod
    def view(filename, objectname):
        om = loader.load(filename)
        om.compute_parents()
        om.collapse_instance_dicts()
        print om.summarize()
        p = om.get_all(objectname)
        # dump the first object
        print p[0]
        # dump all references of the first object
        print p[0].c
        # dump all referrers of the first object
        print p[0].p

if __name__ == "__main__":
    import sys
    funcs = ["load", "view"]
    if len(sys.argv) < 3 or (sys.argv[1] not in funcs):
        print "Usage: python %s %s args" % (__file__, "/".join(funcs))
        sys.exit(0)
    if sys.argv[1] == "load":
        MemObject.load(sys.argv[2])
    else:
        MemObject.view(sys.argv[2], sys.argv[3])
