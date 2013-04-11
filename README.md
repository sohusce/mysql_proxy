mysql_proxy
===========

MySQL Proxy in SCE (Sohu Cloud Engine)

weibo: [http://weibo.com/sohusce]

python and its dependencies:

python version

- 2.6/2.7

python modules

- *** sce_token ***
- MySQLdb
 - mysql
 - mysql-devel
 - python-devel
- DBUtils
- Twisted
- cjson
- zookeeper
- gevent
 - libevent
- pika
- python-memcached


**How to install python extension sce_token ?**

cd sql/

python setup.py install

sql/src/*.[ch] are source codes extracted from [http://dev.mysql.com/downloads/mysql-proxy/]

all directories and files:
<pre>
.
├── docs
│   ├── commands_interaction.png
│   ├── mysql_1_wave.png
│   ├── mysql_3_handshakes.png
│   └── mysql_5_handshakes.png
├── proxy
│   ├── dbinfo_znode.py
│   ├── helper
│   │   ├── aes_helper.py
│   │   ├── constants.py
│   │   ├── db_helper.py
│   │   ├── __init__.py
│   │   ├── ip_helper.py
│   │   ├── mc_helper.py
│   │   ├── mq_helper.py
│   │   ├── tk_helper.py
│   │   ├── utils.py
│   │   └── zk_helper.py
│   ├── __init__.py
│   ├── listen.py
│   ├── mysql_misc.py
│   ├── mysql_packet.py
│   ├── objects_dump.py
│   ├── RWClient.py
│   ├── sce_token.py
│   ├── ServerProtocol.py
│   ├── sql_parser.py
│   ├── sync_mc_pool.py
│   └── threads.py
├── README.md
├── scripts
│   └── start_proxy.sh
└── sql
    ├── Makefile
    ├── sce_token.py
    ├── setup_posix.py
    ├── setup.py
    └── src
        ├── chassis-exports.h
        ├── glib-ext.c
        ├── glib-ext.h
        ├── glib-ext-ref.c
        ├── glib-ext-ref.h
        ├── sql-tokenizer.c
        ├── sql-tokenizer.h
        ├── sql-tokenizer-keywords.c
        ├── sql-tokenizer-keywords.h
        ├── sql-tokenizer.l
        ├── sql-tokenizer-python.c
        ├── sql-tokenizer-tokens.c
        └── sys-pedantic.h

6 directories, 45 files
</pre>
[http://weibo.com/sohusce]: http://weibo.com/sohusce
[http://dev.mysql.com/downloads/mysql-proxy/]: http://dev.mysql.com/downloads/mysql-proxy/
