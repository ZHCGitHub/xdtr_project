#! /bin/bash
#--------------------------------------------
# 这是一个使用sqoop导数据的脚本：
# 
# 功能：从mysql数据库向hive的两个维表导数据
# 特色：
#--------------------------------------------

#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#读取config配置文件，将配置文件load进环境中，成为环境变量 
source ${shell_dir}/config 

#判断hdfs上是否存在目录tb_bs_dic_reg，如果存在则删除并重新创建
hadoop fs -test -e /apps/hive/warehouse/tb_bs_dic_reg
if [ $? -eq 0 ]
then
	hadoop fs -rm -r /apps/hive/warehouse/tb_bs_dic_reg
fi
hadoop fs -mkdir /apps/hive/warehouse/tb_bs_dic_reg

#使用sqoop从mysql中向hive的TB_BS_DIC_REG导入数据
sqoop import --connect jdbc:mysql://192.168.12.38:3306/asmc --username root --password root123 --query 'SELECT DISTINCT businessSys_registeredId,allOriginal_assetName FROM asmc_statistics_business_original_all where $CONDITIONS' --split-by RegisteredId --fields-terminated-by '#' --hive-import -m 1 --hive-table TB_BS_DIC_REG --target-dir /apps/hive/warehouse/tb_bs_dic_reg/data;


#判断hdfs上是否存在目录tb_bs_dic_photo，如果存在则删除并重新创建
hadoop fs -test -e /apps/hive/warehouse/tb_bs_dic_photo
if [ $? -eq 0 ]
then
	hadoop fs -rm -r /apps/hive/warehouse/tb_bs_dic_photo
fi
hadoop fs -mkdir /apps/hive/warehouse/tb_bs_dic_photo

#使用sqoop从mysql中向TB_BS_DIC_PHOTO中导数据
sqoop import --connect jdbc:mysql://192.168.12.10:3306/xdtrdata --username root --password 123456 --query 'SELECT b.catalog_id,a.catalog_name indexname,b.label_type,b.label FROM tb_bs_dic_label_catalog a,tb_bs_dic_label b,tb_bs_dic_caliber c WHERE $CONDITIONS AND a.id = b.catalog_id AND b.id = c.label_id AND a.static_type_id = "USERPHOTO" GROUP BY a.catalog_name, b.label ORDER BY a.target_index' --fields-terminated-by '#' --hive-import -m 1 --hive-table TB_BS_DIC_PHOTO --target-dir /apps/hive/warehouse/tb_bs_dic_photo/data;



