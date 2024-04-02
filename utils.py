import threading
import pymysql
import subprocess
import re
from sys import stderr
import datetime
from logging import getLogger, StreamHandler, Formatter, INFO, DEBUG, ERROR, FileHandler
import settings

def getLog():
    '''
    获得日志对象
    '''
    logger = getLogger(f'log_{datetime.datetime.now()}')
    logger.setLevel(INFO)
    rf_handler = StreamHandler(stderr)  # 默认是sys.stderr
    rf_handler.setLevel(DEBUG)
    f2_handler = FileHandler(settings.log_name)
    f2_handler.setLevel(INFO)
    f2_handler.setFormatter(Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
    f3_handler = FileHandler(settings.error_log_name)
    f3_handler.setLevel(ERROR)
    f3_handler.setFormatter(Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
    logger.addHandler(rf_handler)
    logger.addHandler(f2_handler)
    logger.addHandler(f3_handler)
    return logger
def clean_type(TYPE):
    TYPE=TYPE.split(' ')[0]
    text=re.sub('\(.*?\)','',TYPE)
    return text
def clear_data(queuename,database, table):
    cmd = f'hdfs dfs -rm -r /user/hive/warehouse/{database}.db/{table}/*'
    result = subprocess.run(cmd, shell=True, encoding='gbk')
    if result.returncode == 0:
        return True, 'success'
    else:
        return True, result.stderr
def clear_today_partition_data(queuename,database, table, partition_column, partition_value):

    cmd = f'hdfs dfs -rm -r /user/hive/warehouse/{database}.db/{table}/{partition_column}={partition_value}/*'
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='gbk')
    if result.returncode == 0:
        return True,'success'
    else:
        return True, result.stderr
def create_partition_if_not_exists(queuename,database, table, partition_column, partition_value):
    cmd = f'hive --hiveconf mapreduce.job.queuename={queuename} -e "USE {database}; ALTER TABLE {table} ADD if not exists PARTITION ({partition_column} = \'{partition_value}\');"'
    result=subprocess.run(cmd, shell=True, encoding='gbk')
    if result.returncode == 0:
        return True, 'success'
    else:
        return False, result.stderr
def get_num(log_path):
    with open(log_path,'r',encoding='utf8') as f:
        text=re.sub('\s','',f.read())
        try:
            read_num=int(re.findall('读出记录总数:(\d+)',text)[0])
        except:
            read_num=-1
        try:
            write_num=int(re.findall('读写失败总数:(\d+)',text)[0])
        except:
            write_num=-1
    return read_num,write_num
def run_datax_job(python,datax_path,config_file,partition):
    log_path=f'logs/{config_file.replace("json/","")}-{partition}.log'
    cmd = f'{python} {datax_path} {config_file} > {log_path}'
    result=subprocess.run(cmd, shell=True, encoding='gbk')
    read_num,write_num=get_num(log_path)
    return result.returncode==0,read_num,write_num
def get_thread_id():
    # 获取当前线程的 ID
    thread_id = threading.get_ident()
    return thread_id

class MysqlDB:
    '''
    mysql操作类
    '''

    def __init__(self, params):
        self.params =params

    def getconn(self):

        try:
            conn = pymysql.connect(**self.params)
        except pymysql.err.OperationalError:
            # 如果没有数据库则创建数据库
            db = self.params.pop('db')
            conn = pymysql.connect(**self.params)
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {db}")
            cursor.close()
            conn.close()
            self.params['db'] = db
            conn = pymysql.connect(**self.params)
        return conn

    @staticmethod
    def get_mysql_field_types(params,table):
        '''
        获取表字段类型
        :param params:
        :return:
        '''
        # 连接 MySQL 数据库
        conn = pymysql.connect(**params)
        cursor = conn.cursor()
        # 获取表的字段类型
        cursor.execute(f"DESCRIBE {table}")
        field_types = {field[0]: field[1] for field in cursor.fetchall()}

        # 关闭连接
        cursor.close()
        conn.close()
        return field_types
    @staticmethod
    def is_alive(params):
        '''
        判断mysql连接是否存活
        '''
        try:
            # 执行一个简单的查询
            conn = pymysql.connect(**params)
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            # 检查查询结果
            result = cursor.fetchone()
            if result[0] == 1:
                return True
            else:
                return False
        except:
            return False
    @classmethod
    def create_table(self ,conn, table_name: str, columns: list, types: list):
        '''
        根据表名、字段、字段类型建表
        '''
        columns_str = ''
        id_str = '`id` INT AUTO_INCREMENT PRIMARY KEY,\n'
        for i, j in enumerate(columns):
            if j == 'id':
                id_str = ''
            columns_str += f'`{j}` {types[i]},\n'
        columns_str = columns_str.strip("\n").strip(",")
        sql = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {id_str}
            {columns_str}
        )ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
        '''
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise ValueError(f'创建表{table_name}失败-error{e}-sql:{sql}')

    def get_types(self, data: list):
        '''
        根据数据的类型转换为mysql中字段类型
        '''
        from datetime import datetime
        # 获取一行数据
        data = data[0]

        def getType(column):
            if isinstance(column, str):
                return 'TEXT'
            elif isinstance(column, datetime):
                return 'datetime'
            elif isinstance(column, int):
                return 'int'
            elif isinstance(column, float):
                return 'double'
            elif isinstance(column, bool):
                return 'bool'
            else:
                raise ValueError(f'不支持字段类型:{type(column)}')

        types = []
        for i in data:
            types.append(getType(i))
        return types
    @classmethod
    def check_table_exists(self ,conn, table_name):
        '''
        查询表是否存在
        '''
        try:
            # 创建游标对象
            cursor = conn.cursor()
            # 执行SHOW TABLES查询
            cursor.execute("SHOW TABLES")
            # 获取返回的所有表名
            tables = cursor.fetchall()
            # 检查目标表是否在返回的结果中存在
            if (table_name,) in tables:
                return True
            else:
                return False
        except:
            return False

    def save(self, data: list, table_name: str, method: str = 'append'):
        '''
        简单便捷的保存方法
        自动建库建表
        支持append、replace模式
        '''
        import pandas as pd
        assert len(data) > 0, 'data不能为空'

        df = pd.DataFrame(data)
        conn =self.getconn()
        cur = conn.cursor()
        data = df.values.tolist()
        columns = df.columns.tolist()
        columns_str = ','.join([f'`{i}`' for i in columns])
        s_str = ','.join(['%s' for i in range(len(columns))])
        sql = f'replace into {table_name}({columns_str}) values ({s_str})'
        if self.check_table_exists(conn ,table_name):
            if method == 'append':
                pass
            else:
                cur.execute(f'drop table {table_name}')
                conn.commit()
                types = self.get_types(data)
                self.create_table(conn ,table_name, columns, types)
        else:
            types = self.get_types(data)
            self.create_table(conn ,table_name, columns, types)
        try:
            cur.executemany(sql, data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise ValueError(f'保存失败-error:{e}-sql:{sql}')

def types_mapping(column_types: dict) -> list:
    columns = []
    mapping = {
        "tinyint": "TINYINT",
        "smallint": "SMALLINT",
        "mediumint": "INT",
        "int": "INT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "decimal": "STRING",
        "char": "STRING",
        "varchar": "STRING",
        "text": "STRING",
        "mediumtext": "STRING",
        "longtext": "STRING",
        "binary": "BINARY",
        "varbinary": "BINARY",
        "blob": "BINARY",
        "mediumblob": "BINARY",
        "longblob": "BINARY",
        "date": "TIMESTAMP",
        "datetime": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "year": "STRING"  # 注意：Hive 没有 year 类型，将其映射为字符串类型
    }
    for field, field_type in column_types.items():
        new_type = mapping.get(clean_type(field_type.lower()), 'string')
        columns.append({
            "name": field,
            "type": new_type
        })
    columns.append({
        "name": "cdc_sync_date",
        "type": "TIMESTAMP"
    })
    return columns

if __name__ == '__main__':
    print(run_datax_job('python','D:/datax/bin/datax.py','json/all/ods_syx.ods_q_db_business_reproduce_t_credit_info.json'))