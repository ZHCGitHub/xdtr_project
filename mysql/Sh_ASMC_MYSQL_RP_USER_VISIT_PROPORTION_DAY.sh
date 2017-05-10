#! /bin/bash
#--------------------------------------------
# 这是一个运行mysql命令的脚本：
# 
# 功能：让用户当天的的操作量与平台当天的平均操作量做对比，如果超过阀值，则获取用户的个人信息
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


#向tb_bs_dic_user_visit_avg_day中插入用户操作量与平台平均操作量的对比数据，如果数据不存在则插入，如果数据存在则更新。
# execute sql start  
mysql -u${mysql_rp_user} -p${mysql_rp_passWord} -h${mysql_rp_ip} -e "  
use ${mysql_rp_database}
REPLACE INTO tb_bs_dic_user_visit_avg_day SELECT
	${date},
	IFNULL(ROUND(AVG(VISIT_COUNT)),0)
FROM
	tb_bs_md_user_app_info_day
WHERE
	DEL_DATE = ${date};
	
REPLACE INTO tb_bs_rp_user_visit_proportion_day SELECT
	c.DEL_DATE,
	c.USER_ID,
	c.USER_NAME,
	c.ORGANIZATION_ID,
	c.ORGANIZATION,
	c.VISIT_COUNT,
	(
		c.VISIT_COUNT - a.VISIT_COUNT_AVG
	) / a.VISIT_COUNT_AVG AS SCALE
FROM
	tb_bs_dic_user_visit_avg_day a
LEFT JOIN tb_bs_dic_user_visit_avg_threshold_day b ON a.DEL_DATE = b.DEL_DATE
LEFT JOIN tb_bs_md_user_app_info_day c ON a.DEL_DATE = c.DEL_DATE
WHERE
	a.DEL_DATE = ${date}
AND (
	c.VISIT_COUNT - a.VISIT_COUNT_AVG
) / a.VISIT_COUNT_AVG >= b.THRESHOLD
ORDER BY
	SCALE DESC;
"  
  

