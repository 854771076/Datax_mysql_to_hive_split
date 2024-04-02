## Datax-MySQL_to_Hive-分库分表-数据同步工具

**简介：** 本文档介绍了一个基于Python编写的工具，用于实现分库分表数据同步的功能。该工具利用了DataX作为数据同步的引擎，并通过Python动态生成配置文件，并调用DataX来执行数据同步任务。本文档将介绍工具的功能、使用方法以及示例。

------

**工具功能：**

- 支持分布式数据同步：可以同时从多个数据库实例读取数据，并将数据写入目标数据库。
- 支持分库分表：可以根据配置文件中的分库分表规则，自动进行数据分片和同步。
- 动态生成配置文件：根据用户提供的配置信息，工具可以动态生成DataX的配置文件，简化配置过程。
- 支持定时任务：可以设置定时任务，定期执行数据同步任务，保持数据的实时性。

------

**使用方法：**

1. 安装Python环境：确保您的系统中已经安装了Python环境。
2. 安装DataX：下载并安装DataX，确保DataX可执行文件在系统的PATH路径下。
3. 配置setting文件，配置mysql连接参数，日志数据库参数，hive连接参数，datax.py路径
4. 下载工具代码：从代码仓库中下载工具的源代码。
5. 运行工具：执行工具的主程序，工具将会根据配置文件生成DataX的配置文件，并执行数据同步任务。

------

**注意事项：**

- 在配置分库分表规则时，请确保源数据库和目标数据库的表结构一致。
- 在配置文件中，所有密码等敏感信息应当谨慎处理，避免泄露。

------

**结论：** 本工具是一个简单而强大的分库分表数据同步工具，通过Python编写，使用方便灵活。通过本文档的介绍，您可以快速了解并使用该工具，实现数据库之间的数据同步任务。

### 一、配置

#### 1.1 配置tables.py

```python
DB = 'ods_syx'
TABLES =[{'name': '撼地股比表',
#mysql库名
'db': 'handi', 
#mysql表名
'table': 'task_syx_conprop', 
#分库起始脚标
'db_start': 0, 
#分库数量
'db_num': 0, 
#分表起始脚标
't_start': 0,
#分表数量
't_num': 0, 
#hive库名
'hive_db':DB, 
#hive表名
'hive_table': 'ods_h_hd_task_syx_conprop', 
#更新字段名
'update_column': 'updated',
#是否有other分表（如ods_q_db_df_bidding_t_bidding_info_other）
'other': False, 
#分区字段名
'partition_column': 'partition_date'}]
```

#### 1.2 配置setting.py

```python
#运行环境
environment = 'dev'
if environment == 'dev':
    READER_MYSQL_CONNECT = {
        'host': '10.255.57.189',
        'port': 23306,
        'user': 'hehe_ro',
        'password': '4OJQt_uU+8I^FUhRmqvK6bi+5uj8Grlx',
        'charset': 'utf8',
    }
    MYSQL_CONNECT = {
        'host': '10.8.16.253',
        'port': 3306,
        'user': 'root',
        'password': 'fiang123',
        'charset': 'utf8',
        'db': 'logs'
    }
    # python版本
    python = 'python'
    DATAX_BIN_PATH = 'd://datax/bin/datax.py'
    HDFS_HOST = '10.255.79.162'
    HDFS_PORT = 8020
    # 日志表名称，会自动建表
    log_tb_name = "t_mysql2ods_executed_sync_log"
    log_name = f'{log_tb_name}.log'
    error_log_name = f'{log_tb_name}.error.log'
    # 只生成 0 生成加运行 1 只运行 2
    RUN_TYPE = 1
    TYPE = 'all'
    # 是否启动MYSQL日志
    MYSQL_LOG = True
else:
    READER_MYSQL_CONNECT = {
        'host': '10.255.79.117',
        'port': 23306,
        'user': 'hehe_ro',
        'password': '4OJQt_uU+8I^FUhRmqvK6bi+5uj8Grlx',
        'charset': 'utf8',
    }
    MYSQL_CONNECT = {
        'host': '10.255.79.163',
        'port': 3306,
        'user': 'etl_sync_user_rw',
        'password': 'LZYd5uG!',
        'charset': 'utf8',
        'db': 'db_ods_etl_dwd_ads_sync_mpp'
    }
    # python版本
    python = 'python'
    DATAX_BIN_PATH = '/home/ods_syx_rw_user/datax/bin/datax.py'
    HDFS_HOST = '10.255.79.161'
    HDFS_PORT = 8020
    # 日志表配置
    log_tb_name = "t_mysql2ods_executed_sync_log"
    log_name = f'{log_tb_name}.log'
    error_log_name = f'{log_tb_name}.error.log'
    # 只生成 0 生成加运行 1 只运行 2
    RUN_TYPE = 1
    TYPE = 'update'
    # 是否启动MYSQL日志
    MYSQL_LOG = True
# 重试次数
Retry = 3
#队列
mapred_job_queue_name = 'root.users.ods_job'
# json存放目录
BASE_JSON_DIR = 'json/'
# 是否删除已有分区
is_del = True
# 最大线程数
max_workers = 10
# reader配置
reader_parameter = {
    "username": READER_MYSQL_CONNECT['user'],
    "password": READER_MYSQL_CONNECT['password'],
    "connection": [
        {"jdbcUrl": [f"jdbc:mysql://{READER_MYSQL_CONNECT['host']}:{READER_MYSQL_CONNECT['port']}?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"]
            , "querySql": []}]
}
# writer配置
writer_parameter = {
    "writeMode": "append",
    "column": [],
    "defaultFS": f"hdfs://{HDFS_HOST}:{HDFS_PORT}",
    "path": "/user/hive/warehouse/%s.db/%s",
    "fileType": "text",
    "fileName": "",
    "fieldDelimiter": "\u0001",

}
# datax主配置
datax_config = {
    "job": {
        "setting": {
            "speed": {"channel": 10,"record": -1}
        },
        "content": []
    }
}

log_data={
    'partition_date':'',
    'database_name':'',
    'table_name':'',
    'executed_way':1,
    'executed_state':'',
    'complit_state':2,
    'start_time':'',
    'end_time':'',
    'local_row_update_time_start':'',
    'local_row_update_time_end':'',
    'numrows':0,
    'remark':'',
    'db_num':0,
    't_num':0,
    't_num_0':0,
    'ods_t_name':'',
    'map_input_nums':0,
}

```

### 二、启动

```sh
python3 ./datax_to_hive_run.py [all|update|other]
```

or

```sh
./bin/start.sh [all|update|other]
```

or

```sh
# 可用该脚本结合crontab配置为定时任务
./bin/start-crontab.sh
```

### 三、停止

```
./bin/stop.sh
```

### 四、Datax json和日志路径

logs/[all|update|other]

json/[all|update|other|crontab]