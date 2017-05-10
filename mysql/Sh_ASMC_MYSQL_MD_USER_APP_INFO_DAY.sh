#! /bin/bash
#--------------------------------------------
# 这是一个运行mysql命令的脚本：
# 
# 功能：将TB_BS_MD_USER_APP_INFO_DAY_TMP的的数据导入到TB_BS_MD_USER_APP_INFO_DAY中
# 作用：将TB_BS_MD_USER_APP_INFO_DAY_TMP中的DEL_DATE从string类型转换成date类型
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


#向TB_BS_MD_USER_APP_INFO_DAY中插入数据，如果数据不存在则插入，如果数据存在则更新。
# execute sql stat  
mysql -u${mysql_rp_user} -p${mysql_rp_passWord} -h${mysql_rp_ip} -e "  
use ${mysql_rp_database}
REPLACE INTO tb_bs_md_user_app_info_day SELECT
	DATE(DEL_DATE),
	REG_ID,
	REG_TYPE,
	USER_ID,
	ORGANIZATION,
	ORGANIZATION_ID,
	USER_NAME,
	VISIT_COUNT,
	VISIT_DISIP_COUNT,
	IP_COUNT_FAMILY,
	IP_COUNT_CITY,
	IP_DISCOUNT_CITY,
	IP_COUNT_PRO,
	IP_DISCOUNT_PRO,
	IP_COUNT_COUNTRY,
	IP_DISCOUNT_COUNTRY,
	IP_COUNT_OTHER,
	IP_DISCOUNT_OTHER,
	VISIT_COUNT_TIME0,
	VISIT_COUNT_TIME1,
	VISIT_COUNT_TIME2,
	VISIT_COUNT_TIME3,
	VISIT_COUNT_WORK0,
	VISIT_COUNT_WORK1,
	VISIT_FLOWS_SUM,
	VISIT_TIMES_SUM,
	ADD_COUNT,
	DEL_COUNT,
	UPD_COUNT,
	QUERY_COUNT,
	OTHER_COUNT,
	null
FROM
	tb_bs_md_user_app_info_day_tmp WHERE DEL_DATE=${date}; 
truncate table	tb_bs_md_user_app_info_day_tmp;
"  
  

