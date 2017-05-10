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


#向TB_BS_RP_USER_OPER_PHOTO_DAY中插入数据，如果数据不存在则插入，如果数据存在则更新。
# execute sql stat  
mysql -u${mysql_rp_user} -p${mysql_rp_passWord} -h${mysql_rp_ip} -e "  
use ${mysql_rp_database}
REPLACE INTO tb_bs_rp_user_oper_photo_day SELECT
	DATE(DEL_DATE),
	USER_ID,
	USER_NAME,
	ORGANIZATION,
	ORGANIZATION_ID,
	PHOTO_IP,
	PHOTO_IP_COUNT,
	PHOTO_POSITION,
	PHOTO_POSITION_COUNT,
	PHOTO_WORK,
	PHOTO_WORK_COUNT,
	PHOTO_TIME,
	PHOTO_TIME_COUNT,
	PHOTO_TIMES,
	PHOTO_TIMES_COUNT,
	PHOTO_FLOWS,
	PHOTO_FLOWS_COUNT,
	PHOTO_LONG,
	PHOTO_LONG_COUNT,
	PHOTO_APPS1,
	PHOTO_APPS2,
	PHOTO_APPS3,
	PHOTO_OPERATIONS,
	PHOTO_OPERATIONS_COUNT,
	PHOTO_MODULAR1,
	PHOTO_MODULAR2,
	PHOTO_MODULAR3
FROM
	tb_bs_rp_user_oper_photo_day_tmp WHERE DEL_DATE=${date}; 
truncate table tb_bs_rp_user_oper_photo_day_tmp;
"  
  

