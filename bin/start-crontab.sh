#!/bin/bash
#coding=UTF-8
run_day=`date "+%Y%m%d"`
base_dir='/home/datax_to_hive_syx'
run_name='datax_to_hive_run.py'
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# 切换到脚本所在目录
cd "$script_dir/.."
nohup python3 ${base_dir}/${run_name} update >> ${base_dir}/logs/crontab/run_${run_day}_crontab.log 2>&1 &