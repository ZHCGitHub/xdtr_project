#! /bin/bash
#--------------------------------------------
# 这是一个创建success.ok文件的脚本：
# 先在本地创建一个success.ok文件，再将本地文件put到hdfs上
# 
#--------------------------------------------
##### 用户配置区 开始 #####
#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#读取config配置文件，将配置文件load进环境中，成为环境变量 
source ${shell_dir}/config 

#获取传入的日期
date=$1
#获取传入的年、月、日
date_year=${date:0:4}
date_month=${date:4:2}
#日期是string类型，例如：一号的日期是01
date_day=${date:6:2}

#yesterday=$(date -d yesterday +%Y%m%d)
#yesterday_year=$(date -d yesterday +%Y)
#yesterday_month=$(date -d yesterday +%m)
#日期是string类型，例如：一号的日期是01
#yesterday_day=$(date -d yesterday +%d)

#将日和月改为int类型
if [ ${date_month} -lt 10 ]
then
	date_month_int=${date_month#*0}
else
	date_month_int=${date_month}
fi

if [ ${date_day} -lt 10 ]
then
	date_day_int=${date_day#*0}
else
	date_day_int=${date_day}
fi

fileName="success.ok"
file="${dir}""${fileName}"
#file="/opt/xdtr_project/ASMC/logs/success.ok"  

##### 用户配置区 结束  #####
#判断文件success.ok是否不存在,如果不存在则创建文件
if [ ! -e "${file}" ]
then
    touch ${file} 
fi

#将hive的tb_asmc_md_app_oper_log表分区目录下的success.ok文件删除，避免重复put
hadoop fs -test -e /apps/hive/warehouse/tb_asmc_md_app_oper_log/year=${date_year}/mon=${date_month_int}/day=${date_day_int}/${fileName}
if [ $? -eq 0 ]
then
	hadoop fs -rm -r /apps/hive/warehouse/tb_asmc_md_app_oper_log/year=${date_year}/mon=${date_month_int}/day=${date_day_int}/${fileName}
fi
#将本地的success.ok文件put进hive的tb_asmc_md_app_oper_log表分区目录下
hadoop fs -put ${file} /apps/hive/warehouse/tb_asmc_md_app_oper_log/year=${date_year}/mon=${date_month_int}/day=${date_day_int}

