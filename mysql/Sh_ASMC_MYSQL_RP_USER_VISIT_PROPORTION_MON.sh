#! /bin/bash
#--------------------------------------------
# 这是一个运行mysql命令的脚本：
# 
# 功能：让用户当月的的操作量与平台当月的平均操作量做对比，如果超过阀值，则获取用户的个人信息
# 特色：
#--------------------------------------------
#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#读取config配置文件，将配置文件load进环境中，成为环境变量 
source ${shell_dir}/config 


#获取传入的日期
date=$1


#向tb_bs_dic_user_visit_avg_mon中插入用户操作量与平台平均操作量的对比数据，如果数据不存在则插入，如果数据存在则更新。
# execute sql start  
mysql -u${mysql_rp_user} -p${mysql_rp_passWord} -h${mysql_rp_ip} -e "  
use ${mysql_rp_database}
REPLACE INTO tb_bs_dic_user_visit_avg_mon SELECT
	DEL_MONTH,
	ROUND(AVG(VISIT_COUNT))
FROM
	tb_bs_md_user_app_info_mon
WHERE
	DEL_MONTH = ${date};
	
REPLACE INTO tb_bs_rp_user_visit_proportion_mon SELECT
	c.DEL_MONTH,
	c.USER_ID,
	c.USER_NAME,
	c.ORGANIZATION_ID,
	c.ORGANIZATION,
	c.VISIT_COUNT,
	(
		c.VISIT_COUNT - a.VISIT_COUNT_AVG
	) / a.VISIT_COUNT_AVG AS SCALE
FROM
	tb_bs_dic_user_visit_avg_mon a
LEFT JOIN tb_bs_dic_user_visit_avg_threshold_mon b ON a.DEL_MONTH = b.DEL_MONTH
LEFT JOIN tb_bs_md_user_app_info_mon c ON a.DEL_MONTH = c.DEL_MONTH
WHERE
	a.DEL_MONTH = ${date}
AND (
	c.VISIT_COUNT - a.VISIT_COUNT_AVG
) / a.VISIT_COUNT_AVG >= b.THRESHOLD
ORDER BY
	SCALE DESC;
"  
  

