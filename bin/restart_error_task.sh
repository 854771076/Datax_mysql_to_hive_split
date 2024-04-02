#!/bin/bash
# 获取脚本所在目录
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# 切换到脚本所在目录
cd "$script_dir/.."
# 获取参数值
param="$1"
# 执行不同的命令
case $param in
    all)
        nohup python3 ./cheak_datax_task_status.py all > retry.log 2>&1 &
        ;;
    update)
        nohup python3 ./cheak_datax_task_status.py update > retry.log 2>&1 &
        ;;
    other)
        nohup python3 ./cheak_datax_task_status.py other $2 $3 $4 > retry.log 2>&1 &
        ;;
    *)
        echo "Usage: $0 [all|update|other]"
        exit 1
        ;;
esac