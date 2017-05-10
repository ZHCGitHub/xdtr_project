#! /bin/bash
#--------------------------------------------
# 这是一个使用sqoop导数据的脚本：
# 
# 功能：从hdfs向mysql上导数据(从hive中的表TB_BS_MD_APP_OPER_UNUSUAL_LOG(用户异常操作信息表)
# 向前台展示mysql数据库导数据)
# 特色：
#--------------------------------------------
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



#判断hdfs上是否存在用户异常操作数据，如果存在数据，则把数据从hdfs上导入到mysql
hadoop fs -test -e /apps/hive/warehouse/tb_bs_ls_app_oper_unusual_log/year=${date_year}/mon=${date_month_int}/day=${date_day_int}
if [ $? -eq 0 ]
then
echo 'hello world'
sqoop export --connect "jdbc:mysql://${mysql_rp_ip}:3306/${mysql_rp_database}?useUnicode=true&characterEncoding=utf-8" --username ${mysql_rp_user} --password ${mysql_rp_passWord} --update-key "ID" --update-mode allowinsert -m 3 --fields-terminated-by '${delimiter}' --table tb_bs_ls_app_oper_unusual_log --export-dir /apps/hive/warehouse/tb_bs_ls_app_oper_unusual_log/year=${date_year}/mon=${date_month_int}/day=${date_day_int}/
fi
