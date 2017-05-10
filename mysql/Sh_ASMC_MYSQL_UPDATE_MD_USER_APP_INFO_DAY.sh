#! /bin/bash
#--------------------------------------------
# 这是一个运行mysql命令的脚本：
# 
# 功能：获取用户前七天（去掉周末）的平均操作量，插入到tb_bs_dic_user_visit_count_day最后一个字段
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
    #date_month_int=${date_month#*0}
	date_month_int=${date_month}
else
    date_month_int=${date_month}
fi

if [ ${date_day} -lt 10 ]
then
    #date_day_int=${date_day#*0}
	date_day_int=${date_day}
else
    date_day_int=${date_day}
fi
# This function expects 1 Arguments,
# YYYYMMDD
# example:20120311
# then
# Returns a value between 0 and 6 to represent the day of the
# week where 0=Sun,1=Mon,...6=Sat
funWithParam(){
	mark=`date -d "${1}" +%w` 
	echo  $mark
	if [ $mark -ne 0 -a $mark -ne 6 ];
	then
		#向tb_bs_dic_user_visit_count_day中插入数据，如果数据不存在则插入，如果数据存在则更新。

		#execute sql start
		mysql -u${mysql_rp_user} -p${mysql_rp_passWord} -h${mysql_rp_ip} -e "  
		use ${mysql_rp_database}
		REPLACE INTO tb_bs_dic_user_visit_count_day SELECT
			DEL_DATE,
			REG_ID,
			USER_ID,
			USER_NAME,
			VISIT_COUNT
		FROM
			tb_bs_md_user_app_info_day
		WHERE
			DEL_DATE = ${1}
		ORDER BY
			VISIT_COUNT DESC;
		"  
	fi	
}

#获取指定日期前七天的日期
date_1_ago=`date -d "${date} 1 days ago " "+%Y%m%d"`
date_2_ago=`date -d "${date} 2 days ago " "+%Y%m%d"`
date_3_ago=`date -d "${date} 3 days ago " "+%Y%m%d"`
date_4_ago=`date -d "${date} 4 days ago " "+%Y%m%d"`
date_5_ago=`date -d "${date} 5 days ago " "+%Y%m%d"`
date_6_ago=`date -d "${date} 6 days ago " "+%Y%m%d"`
date_7_ago=`date -d "${date} 7 days ago " "+%Y%m%d"`
#向中间表tb_bs_dic_user_visit_count_day导入前七天的数据
funWithParam ${date_1_ago}
funWithParam ${date_2_ago}
funWithParam ${date_3_ago}
funWithParam ${date_4_ago}
funWithParam ${date_5_ago}
funWithParam ${date_6_ago}
funWithParam ${date_7_ago}

#更新tb_bs_md_user_app_info_day表的OPP_COUNT_AV5字段（取该用户前七天的数据（去除周末）/5）,
# 清空临时表tb_bs_md_user_app_info_day_tmp
mysql -u${mysql_rp_user} -p${mysql_rp_passWord} -h${mysql_rp_ip} -e "
use ${mysql_rp_database}
UPDATE tb_bs_md_user_app_info_day a
		INNER JOIN (
	        SELECT
		        ${date} AS DEL_DATE,
		        REG_ID,
		        USER_ID,
		        USER_NAME,
		        SUM(VISIT_COUNT) / 5 AS VISIT_COUNT_AVG
	        FROM
		        tb_bs_dic_user_visit_count_day
	        GROUP BY
	            REG_ID,
		        USER_ID,
		        USER_NAME
        ) b ON a.DEL_DATE = b.DEL_DATE
        AND a.USER_ID = b.USER_ID
        AND a.USER_NAME = b.USER_NAME
        AND a.REG_ID = b.REG_ID
        SET a.OPP_COUNT_AV5 = b.VISIT_COUNT_AVG;

        truncate table	tb_bs_dic_user_visit_count_day;
"