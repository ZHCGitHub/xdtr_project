#! /bin/bash
#--------------------------------------------
# 这是一个使用sqoop导数据的脚本：
# 
# 功能：从mysql数据库向hdfs上导数据
# 特色：
#--------------------------------------------

#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#读取config配置文件，将配置文件load进环境中，成为环境变量 
source ${shell_dir}/config 

#获取传入的日期
date=$1
#获取传入的年、月
date_year=${date:0:4}
date_month=${date:4:2}

#将月改为int类型
if [ ${date_month} -lt 10 ]
then
	#date_month_int=${date_month#*0}
	date_month_int=${date_month}
else
	date_month_int=${date_month}
fi

#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#读取config配置文件，将配置文件load进环境中，成为环境变量 
source ${shell_dir}/config 

#判断hdfs上是否存在用户操作习惯画像汇总月表的数据标示(success.ok)，如果存在数据，则把数据从hdfs上导入到mysql
hadoop fs -test -e /xdtrdata/ASMC/md/userPhotoMon/${date_year}/${date_month}/success.ok
if [ $? -eq 0 ]
then
	sqoop export --connect "jdbc:mysql://${mysql_rp_ip}:3306/${mysql_rp_database}?useUnicode=true&characterEncoding=utf-8" --username ${mysql_rp_user} --password ${mysql_rp_passWord} --update-key "DEL_DATE,USER_ID,USER_NAME" --update-mode allowinsert -m 3 --fields-terminated-by '${delimiter}' --table tb_bs_rp_user_oper_photo_mon --export-dir /xdtrdata/ASMC/md/userPhotoMon/${date_year}/${date_month}/

fi
