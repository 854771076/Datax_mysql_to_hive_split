"""
@File        :  datax_to_hive.py
@Contact     :  fuyang854771076@gmail.com
@Author      :  fuyang
@Desciption  :  mysql数据拉取到hive 支持三种模式：
    ## 运行
    ## cd bin
    ##三种拉取方式
    ##1 全量  截止运行时间零点  ./start.sh all
    ##2 增量  运行时间前一天24小内 ./start.sh update
    ##3 指定时间段 追加放入昨天分区/指定分区里 ./start.sh other  "2022-07-01 00:00:00"  "2022-07-02 00:00:00"  "2022-07-01"

"""

import datetime, sys, time
import json
from utils import *
import settings
import concurrent.futures
from tables import TABLES
import functools
import copy



def get_datax_json(t1: dict):
    '''
    生成json
    '''
    table = copy.deepcopy(t1)
    log_data = copy.deepcopy(settings.log_data)
    log_data['ods_t_name'] = table.get('hive_table')
    log_data['start_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_data['db_num'] = table['db_num']
    log_data['t_num'] = table['t_num']
    # 单库单表
    if table['db_num'] == 0 and table['t_num'] == 0:
        table['tables'] = [table["db"] + '.' + table['table']]
    # 单库多表
    elif table['db_num'] == 0 and table['t_num'] > 0:
        '''
        程序自动添加表下标并且依次遍历分表
        '''
        table['tables'] = []
        for i in range(table['t_start'], table['t_num'] + 1):
            table['tables'].append(table["db"] + '.' + table['table'] + f'_{i}')
        if table.get('other'):
            table['tables'].append(f'{table["db"]}.{table["table"]}_other')
    # 多库多表
    elif table['db_num'] > 0 and table['t_num'] > 0:
        '''
        程序自动添加库和表下标并且依次遍历分表
        '''
        table['tables'] = []
        for i in range(table['db_start'], table['db_num'] + 1):
            for j in range(table['t_start'], table['t_num'] + 1):
                table['tables'].append(f'{table["db"]}_{i}.{table["table"]}_{j}')
            if table.get('other'):
                table['tables'].append(f'{table["db"]}_{i}.{table["table"]}_other')
    ##不能识别类型
    else:
        raise ValueError('db_num or t_num ValueError')
    '''
        生成将mysql数据导入hive的json，并记录日志
    '''
    name = get_thread_id()
    datax_config = copy.deepcopy(settings.datax_config)
    reader_parameter = copy.deepcopy(settings.reader_parameter)
    writer_parameter = copy.deepcopy(settings.writer_parameter)
    # 保存json脚本
    script_path = settings.BASE_JSON_DIR + f"{settings.TYPE}/{table['hive_db']}.{table['hive_table']}.json"
    try:
        if settings.RUN_TYPE in (0, 1):
            settings.logger.info(
                f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},json生成开始")
            column_types = MysqlDB.get_mysql_field_types(settings.READER_MYSQL_CONNECT, table['tables'][0])
            log_data['database_name'] = table.get('db')
            log_data['table_name'] = table.get('table')
            if settings.TYPE == 'all':
                where = f"{table.get('update_column', '')}<'{settings.today_time.strftime('%Y-%m-%d 00:00:00')}'"
            elif settings.TYPE == 'update':
                where = f"{table.get('update_column', '')} between '{settings.yesterday_time.strftime('%Y-%m-%d 00:00:00')}' and '{settings.today_time.strftime('%Y-%m-%d 00:00:00')}'"
            elif settings.TYPE == 'other':
                where = f"{table.get('update_column', '')} between '{start_time}' and '{end_time}'"
            else:
                where = ""
            if table.get('update_column', '') != '' and table.get('partition_column', "") != "":
                reader_parameter['connection'][0]['querySql'] = [
                    f"select *,now() as cdc_sync_date from {i} where {where}" for i in table['tables']]
            else:
                reader_parameter['connection'][0]['querySql'] = [f"select *,now() as cdc_sync_date from {i}" for i in
                                                                 table['tables']]

            # 补齐writer
            if table.get('partition_column', "") != "":
                writer_parameter["path"] = writer_parameter["path"] % (
                    table['hive_db'], table['hive_table']) + f"/{table.get('partition_column')}={settings.partition_date}"
            else:
                writer_parameter["path"] = writer_parameter["path"] % (
                    table['hive_db'], table['hive_table'])
            writer_parameter["column"] = types_mapping(column_types)
            writer_parameter["fileName"] = table['hive_table']
            datax_config["job"]["content"] = [{
                "reader": {"name": "mysqlreader", "parameter": reader_parameter},
                "writer": {"name": "hdfswriter", "parameter": writer_parameter}
            }]


            with open(script_path, 'w') as f:
                json.dump(datax_config, f, indent=4)
            settings.logger.info(
                f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},json生成成功")
        if settings.RUN_TYPE in (1, 2):
            if table.get('partition_column', "") != "":
                # 如果没有分区，则新增分区
                status, err = create_partition_if_not_exists(settings.mapred_job_queue_name, table['hive_db'],
                                                             table['hive_table'], table['partition_column'],
                                                             settings.partition_date)
                if not status:
                    log_data['executed_state'] = 'success' if status else 'failed'
                    log_data['complit_state'] = 1 if status else 2
                    log_data['remark'] = '创建分区失败'
                    settings.logger.error(
                        f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},msg:创建分区失败-error:{err}")
                    return
                # 清空昨天分区内容
                if settings.is_del:
                    status, err = clear_today_partition_data(settings.mapred_job_queue_name, table['hive_db'],
                                                             table['hive_table'], table['partition_column'],
                                                             settings.partition_date)
                    if not status:
                        log_data['executed_state'] = 'success' if status else 'failed'
                        log_data['complit_state'] = 1 if status else 2
                        log_data['remark'] = '清空分区失败'
                        settings.logger.error(
                            f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},msg:清空分区失败-error:{err}")
                        return
            else:
                # 清空内容
                if settings.is_del:
                    status, err = clear_data(settings.mapred_job_queue_name, table['hive_db'], table['hive_table'])
                    if not status:
                        log_data['executed_state'] = 'success' if status else 'failed'
                        log_data['complit_state'] = 1 if status else 2
                        log_data['remark'] = '清空表失败'
                        settings.logger.error(
                            f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},msg:清空表失败,error:{err}")
                        return
            # 运行脚本
            settings.logger.info(
                f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},json正在执行")
            status, read_num, write_error_num = run_datax_job(settings.python, settings.DATAX_BIN_PATH, script_path,
                                                        settings.partition_date)
            # 重试
            if not status or write_error_num!=0:
                for Retry in range(settings.Retry):
                    status, read_num, write_error_num = run_datax_job(settings.python, settings.DATAX_BIN_PATH, script_path,
                                                                settings.partition_date)
                    if status and write_error_num==0:
                        break
                    else:
                        settings.logger.warning(f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},json执行失败，正在重试")
            if status and write_error_num==0:
                log_data['executed_state'] = 'success'
                log_data['complit_state'] = 1
                log_data['numrows'] = read_num
                settings.logger.info(
                    f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},json执行成功")
                log_data[
                    'remark'] = f"{table['db']}.{table['table']}数据到hive表{table['hive_db']}.{table['hive_table']},json执行成功"
            else:
                log_data['executed_state'] ='failed'
                log_data['complit_state'] =2
                log_data['numrows'] = read_num-write_error_num

                settings.logger.error(
                    f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},json执行失败，请查看日志")
                log_data[
                    'remark'] = f"{table['db']}.{table['table']}数据到hive表{table['hive_db']}.{table['hive_table']},json执行失败，请查看日志"
    except Exception as e:
        settings.logger.error(
            f"Thread {name}-{settings.TYPE}: {table['db']}.{table['table']}->{table['hive_db']}.{table['hive_table']},json执行失败----error:{e}")
        log_data['remark'] = f"{table['db']}.{table['table']}json执行失败----error:{e}"
    finally:
        log_data['end_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if settings.MYSQL_LOG:
            settings.Log_db.save([log_data], settings.log_tb_name, 'append')


if __name__ == '__main__':
    settings.logger = getLog()
    settings.today_time = datetime.date.today()
    settings.yesterday_time = settings.today_time + datetime.timedelta(-1)
    settings.partition_date = settings.yesterday_time.strftime("%Y%m%d")

    settings.Log_db = MysqlDB(settings.MYSQL_CONNECT)
    try:
        TYPE = sys.argv[1]
        if TYPE not in ['all', 'update', 'other']:
            TYPE = settings.TYPE
        else:
            settings.TYPE=TYPE
    except:
        TYPE = settings.TYPE
    tables = TABLES
    settings.logger.info(f'生成开始--type:{TYPE}')
    start = time.time()
    ##1 全量  截止运行时间零点
    settings.logger.info('mysql_to_hive start')
    if TYPE == 'all':
        settings.log_data['executed_way'] = 1
        settings.log_data['partition_date'] = settings.partition_date
        settings.log_data['local_row_update_time_start'] = settings.yesterday_time.strftime("%Y-%m-%d 00:00:00")
        settings.log_data['local_row_update_time_end'] = settings.today_time.strftime("%Y-%m-%d 00:00:00")

    elif TYPE == 'update':
        settings.log_data['executed_way'] = 0
        settings.log_data['partition_date'] = settings.partition_date
        settings.log_data['local_row_update_time_start'] = settings.yesterday_time.strftime("%Y-%m-%d 00:00:00")
        settings.log_data['local_row_update_time_end'] = settings.today_time.strftime("%Y-%m-%d 00:00:00")

    ##3 指定时间段 追加放入昨天分区/指定分区里
    elif TYPE == 'other':
        start_time = sys.argv[2]
        end_time = sys.argv[3]
        # start_time = "2022-08-11 00:00:00"
        # end_time   = "2022-08-12 00:00:00"
        settings.partition_date = sys.argv[4] if len(sys.argv) > 4 else settings.partition_date
        settings.log_data['partition_date'] = settings.partition_date
        settings.log_data['executed_way'] = 2
        settings.log_data['local_row_update_time_start'] = start_time
        settings.log_data['local_row_update_time_end'] = end_time
    else:
        raise ValueError(f'不能识别类型{TYPE}')

    with concurrent.futures.ThreadPoolExecutor(max_workers=settings.max_workers) as executor:
        # 提交每个表的处理任务到线程池
        futures = [executor.submit(functools.partial(get_datax_json, table)) for table in tables]

        # 等待所有任务完成
        concurrent.futures.wait(futures)
    end = time.time()
    settings.logger.info(f'导入完成---总耗时{(end - start)}秒')
