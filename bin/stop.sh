#!/bin/bash
kid=`ps -ef |grep run.py| grep -v "grep"|awk '{print $2}'`
kill -9 $kid
if [ "$?" -eq 0 ]; then
    echo "kill成功，pid:"$kid
else
    echo "kill失败，没有找到对应的进程"
fi