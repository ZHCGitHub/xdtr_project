#! /bin/bash
#--------------------------------------------
# 这是一个保存shell脚本日志的脚本：
# 
# 功能：保存shell脚本中命令的日志
# 特色：
#--------------------------------------------
##### 用户配置区 开始 #####
#当前时间
date=$(date +%Y%m%d)
#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#读取config配置文件，将配置文件load进环境中，成为环境变量 
#source ${shell_dir}/config  

#log="${dir}shell_${date}.log"
#log="/opt/xdtr_project/ASMC/logs/shell_20120918"   #操作日志存放路径 

##### 用户配置区 结束  #####
#生成时间维表和城市维表的文本文件
java -jar write.jar

#将两个文本文件上传至hadoop集群
hadoop fs -put ./ip /xdtrdata/ASMC/
hadoop fs -put ./time /xdtrdata/ASMC/

#调用python脚本将它们insert进hive的表中
python /opt/xdtr_project/ASMC/python/bin/Py_ASMC_CreateTable.py

#将前台展示的两个维表数据，用sqoop导入到hive中
sh /opt/xdtr_project/ASMC/shell/Sh_ASMC_Sqoop_DIC.sh

