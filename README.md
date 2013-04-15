mysql_proxy
===========
MySQL Proxy in SCE (Sohu Cloud Engine)

简介
-----------
> 搜狐云引擎（SCE，Sohu Cloud Engine）是搜狐公司推出的PaaS平台，目前处于私有云阶段，在不久的将来将发布公有云，敬请期待。
> MySQL Proxy 是 SCE 不可或缺的部分，它隔离了应用和数据库服务器，便于 SCE 集中管理数据库配置信息，并对数据库资源进行统计和限制，给服务计费和日志报表等提供元数据。

功能
-----------
- MySQL 握手协议、挥手协议和请求协议的解析
- 基本请求转发
- 私有 SQL
- 自定义错误信息
- 客户端 IP 白名单
- SQL 白名单
- 读写分离
- 连接保持和释放
- 数据库动态配置
- 流量统计限制
- 连接数统计限制
- 数据库配额限制

Python 版本及其依赖
-----------
- Python 2.7.3
- sce_token
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

sce_token 模块
-----------
> sce_token 模块封装 MySQL 官方数据库代理 mysql-proxy 的词法分析部分，见[http://dev.mysql.com/downloads/mysql-proxy/]

 **sce_token 安装**

```cd sql/
python setup.py install```

其他
-----------
有问题请联系 [http://weibo.com/sohusce]

项目目录结构：
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