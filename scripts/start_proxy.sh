#!/usr/bin/env bash
######################################################################
## Filename:      start_proxy.sh
##                
## Copyright (C) 2012,  Sohu Cloud Engine @sohu.com
## Version:       
## Author:        fifar <fifarzh@gmail.com>
## Created at:    Wed Feb 29 10:30:05 2012
##                
## Description:   
##                
######################################################################
SERVICE=sce_db

serv_dir=/opt/services/$SERVICE
conf_dir=/opt/conf/$SERVICE
logs_dir=/opt/logs/$SERVICE

git_repo=watermelon
dir_proxy=proxy
logpid_proxy=sce_db_proxy

path_python=/opt/apps/python/bin/python
path_twistd=/opt/apps/python/bin/twistd
path_daemon=/opt/apps/daemontools/command

test $# -ne 1 && echo -e "Arguments Error in start_master_stats\nUsage: sh $0 local/server_local/dev/test/release" && exit
case $1 in
    local)
        ;;
    server_local)
        ;;
    dev)
	    ;;
    test)
	    ;;
    release)
	    ;;
    *)
        echo -e "Usage: sh $0 local/server_local/dev/test/release" && exit
	    ;;
esac


if [ "x$1" == "xlocal" ]; then
    path_python=/usr/bin/python
    path_twistd=/usr/bin/twistd
    serv_dir=/root/code
fi

if [ "x$1" == "xlocal" ]; then
    [ ! -d $conf_dir ] && mkdir -p $conf_dir
    [ ! -d $logs_dir ] && mkdir -p $logs_dir
    [ ! -d $serv_dir ] && mkdir -p $serv_dir
else
    [ ! -d $conf_dir ] && mkdir -p $conf_dir || chown app:root $conf_dir -R
    [ ! -d $logs_dir ] && mkdir -p $logs_dir || chown app:root $logs_dir -R
    [ ! -d $serv_dir ] && mkdir -p $serv_dir || chown app:root $serv_dir -R
fi

# -d stop supervise; -x exit when service is down
source /etc/profile
$path_daemon/svc -dx $serv_dir/$git_repo/proxy
sleep 1

[ -f $conf_dir/${logpid_proxy}.pid ] && kill -INT `cat $conf_dir/${logpid_proxy}.pid` && sleep 1 && kill -KILL `cat $conf_dir/${logpid_proxy}.pid`
function kill_pid {
    if [ $# -ne 1 ]; then exit 0; fi
    pid=`ps aux | grep $1 | grep -v grep | awk '{print $2}'`
    if [ "x$pid" != "x" ]; then
        kill -INT $pid
        sleep 1
        kill -KILL $pid
    fi
}

kill_pid listen.py
sleep 6

cd $serv_dir/$git_repo/$dir_proxy
cat > $serv_dir/$git_repo/proxy/run <<EOF
#!/bin/bash
sleep 30
echo "`date +'%Y-%m-%d %H:%M:%S'` reboot sce_db_proxy" >> $conf_dir/reboot.log
$path_python $path_twistd --prefix=$1 --nodaemon --no_save --reactor=poll --python=$serv_dir/$git_repo/$dir_proxy/listen.py --pidfile=$conf_dir/${logpid_proxy}.pid --logfile=$logs_dir/twistd_${logpid_proxy}.log
EOF
chmod +x $serv_dir/$git_repo/proxy/run
nohup $path_daemon/supervise $serv_dir/$git_repo/proxy >/dev/null 2>&1 &
