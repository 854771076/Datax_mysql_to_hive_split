
environment = 'dev'
if environment == 'dev':
    # 源数据数据库
    READER_MYSQL_CONNECT = {
        'host': 'xxx',
        'port': 3306,
        'user': 'xxx',
        'password': 'xxx',
        'charset': 'utf8',
    }
    # 日志数据库
    MYSQL_CONNECT = {
        'host': 'xxx',
        'port': 3306,
        'user': 'xxx',
        'password': 'xxx',
        'charset': 'utf8',
        'db': 'logs'
    }
    # python版本
    python = 'python'
    DATAX_BIN_PATH = 'd://datax/bin/datax.'
    HDFS_HOST = '10.255.79.162'
    HDFS_PORT = 8020
    # 日志表表名，会自动建表
    log_tb_name = "t_mysql2ods_executed_sync_log"
    log_name = f'{log_tb_name}.log'
    error_log_name = f'{log_tb_name}.error.log'
    # 只生成 0 生成加运行 1 只运行 2
    RUN_TYPE = 1
    TYPE = 'update'
    # 是否启动MYSQL日志，如果True必须配置MYSQL_CONNECT
    MYSQL_LOG = True
else:
    # 源数据数据库
    READER_MYSQL_CONNECT = {
        'host': 'xxx',
        'port': 3306,
        'user': 'xxx',
        'password': 'xxx',
        'charset': 'utf8',
    }
    # 日志数据库
    MYSQL_CONNECT = {
        'host': 'xxx',
        'port': 3306,
        'user': 'xxx',
        'password': 'LZYd5uG!',
        'charset': 'utf8',
        'db': 'xxx'
    }
    # python版本
    python = 'python'
    DATAX_BIN_PATH = '/home/ods_syx_rw_user/datax/bin/datax.py'
    HDFS_HOST = '10.255.79.161'
    HDFS_PORT = 8020
    # 日志表表名，会自动建表
    log_tb_name = "t_mysql2ods_executed_sync_log"
    log_name = f'{log_tb_name}.log'
    error_log_name = f'{log_tb_name}.error.log'
    # 只生成 0 生成加运行 1 只运行 2
    RUN_TYPE = 1
    TYPE = 'update'
    # 是否启动MYSQL日志，如果True必须配置MYSQL_CONNECT
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
