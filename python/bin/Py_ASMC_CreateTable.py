#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：CreateTable
# **  功能描述：创建hive数据分析所用的四个表,并向两个维表中load数据
# **  输入：    /xdtrdata/ASMC/ip    /xdtrdata/ASMC/time
# **                  
# **  输出:  原始日志表：          tb_asmc_ls_app_oper_log
# **        ip维表：                tb_asmc_dic_ip_area_code
# **        时间维表：              tb_asmc_dic_time_catalog
# **        中度汇总表：          tb_asmc_md_app_oper_log
# **  创建者: 
# **  电话:
# **  创建日期: 
# **  修改日志:
# **  修改日期:
# **  修改人 :
# **  修改内容:
# ** ---------------------------------------------------------------------------------------
# **  
# ** ---------------------------------------------------------------------------------------
# **  
# **  程序调用格式：python CreateTable.py
# **  调用次数：  一次  
#********************************************************************************************
# **  Copyright(c) 2015 , Inc. 
# **  All Rights Reserved.
#********************************************************************************************
import sys
from Py_Con_Hive import HiveExe
import os
from lxml import etree
try:
    #*************************************************************************************************************
    #读取XML文件中的配置数据
    #*************************************************************************************************************
    # 由于config.xml放置在与当前文件相同的目录下，因此通过 __file__ 来获取XML文件的目录，然后再拼接成绝对路径
    # 这里利用了lxml库来解析XML
    root = etree.parse(os.path.join(os.path.dirname(__file__), 'python-hive-site.xml')).getroot()
    root1 = etree.parse(os.path.join(os.path.dirname(__file__), 'py-logs-site.xml')).getroot()
    #获取配置文件中的信息，并去除空格
    delimiters = root.find('delimiter').text.lstrip().rstrip()
    if (delimiters=="\\01"):
        delimiter=""
    else:
        delimiter=delimiters
    #delimiter = "\01"
    #*************************************************************************************************************
    #SQL体，程序开始处理
    #*************************************************************************************************************
    hivesql = []
    #**********************************
    #程序运行(生成原始日志表:tb_asmc_ls_app_oper_log)
    #**********************************    
    sql1='''
    create external table if not exists TB_ASMC_LS_APP_OPER_LOG(
    id int,
    Num_id string,
    Reg_id string,
    User_id string,
    Organization string,
    Organization_id string,
    User_name string,
    Operate_time string,
    ResponePackage int,
    Terminal_id string,
    Operate_type int,
    Operate_result int,
    Error_code string,
    Operate_name string,
    Operate_condition string,
    Insert_time string,
    Operate_date int,
    Index_time string,
    Update_time string) partitioned by (year int,mon int,day int)
    row format delimited 
    fields terminated by '%s'
    ''' %(delimiter)
    hivesql.append(sql1)
    #**********************************
    #程序运行(生成ip维表:tb_asmc_dic_ip_area_code)
    #**********************************    
    sql2='''
    create table if not exists TB_ASMC_DIC_IP_AREA_CODE(
    Area_code int,
    AREA_NAME string,
    Prov_name string,
    Type_Provence int,
    Ip_First int,
    Ip_Second int,
    Ip_Third int)
    row format delimited 
    fields terminated by '#'
    ''' 
    hivesql.append(sql2)
    #**********************************
    #程序运行(向ip维表load数据)
    #**********************************
    load1='''
    LOAD DATA INPATH '/xdtrdata/ASMC/ip' OVERWRITE INTO TABLE TB_ASMC_DIC_IP_AREA_CODE
    '''
    hivesql.append(load1)
    #**********************************
    #程序运行(生成时间维表:tb_asmc_dic_time_catalog)
    #**********************************
    sql3='''
    create table if not exists TB_ASMC_DIC_TIME_CATALOG(
    Flag_week int, 
    Time_Hour int,
    Time_Minute int,
    Flag_Time int,
    Flag_Work int)
    row format delimited 
    fields terminated by '#'
    '''
    hivesql.append(sql3)
    #**********************************
    #程序运行(向时间维表load数据)
    #**********************************
    load2='''
    LOAD DATA INPATH '/xdtrdata/ASMC/time' OVERWRITE INTO TABLE TB_ASMC_DIC_TIME_CATALOG
    '''
    hivesql.append(load2)
    #**********************************
    #程序运行(生成中度汇总表:tb_asmc_md_app_oper_log)
    #**********************************
#     sql='''
#     create external table if not exists TB_ASMC_MD_APP_OPER_LOG(
#     id int,
#     Num_id string,
#     Reg_id string,
#     User_id string,
#     Organization string,
#     Organization_id string,
#     User_name string,
#     Operate_time string,
#     Terminal_id string,
#     Operate_type int,
#     Operate_result int,
#     Error_code string,
#     Operate_name string,
#     Operate_condition string,
#     Operate_date int,
#     Area_code int,
#     Area_name string,
#     Prov_name string,
#     Type_Provence int,
#     Flag_time int,
#     Flag_work int) partitioned by (dt int)
#     row format delimited 
#     fields terminated by '$'
#     '''
    #**********************************
    #修改建表语句，为表定义多字符分隔符输出
    #修改人：宋增旭
    #修改时间：20160930
    #**********************************  
#     sql4='''
#     create external table if not exists TB_ASMC_MD_APP_OPER_LOG(
#     id int,
#     Num_id string,
#     Reg_id string,
#     User_id string,
#     Organization string,
#     Organization_id string,
#     User_name string,
#     Operate_time string,
#     Terminal_id string,
#     Operate_type int,
#     Operate_result int,
#     Error_code string,
#     Operate_name string,
#     Operate_condition string,
#     Operate_date int,
#     Area_code int,
#     Area_name string,
#     Prov_name string,
#     Type_Provence int,
#     Flag_work int,
#     Flag_time int) partitioned by (year int,mon int,day int)
#     row format delimited 
#     fields terminated by '%s'
#     '''%(delimiter)
#     hivesql.append(sql4)
    
    #**********************************
    #修改建表语句，为表TB_ASMC_MD_APP_OPER_LOG添加businessOSysLog_responePackage(回复包大小)字段
    #修改人：宋增旭
    #修改时间：20161212
    #**********************************  
    sql4='''
    create external table if not exists TB_ASMC_MD_APP_OPER_LOG(
    id int,
    Num_id string,
    Reg_id string,
    User_id string,
    Organization string,
    Organization_id string,
    User_name string,
    Operate_time string,
    Terminal_id string,
    Operate_type int,
    Operate_result int,
    Error_code string,
    Operate_name string,
    Operate_condition string,
    Insert_time string,
    Area_code int,
    Area_name string,
    Prov_name string,
    Type_Provence int,
    Flag_work int,
    Flag_time int,
    ResponePackage int) partitioned by (year int,mon int,day int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql4)
    #**********************************
    #修改TB_ASMC_MD_APP_OPER_LOG中的'\N'为''
    #修改人：宋增旭
    #修改时间：20160930
    #**********************************
    sql5='''
    alter table tb_asmc_md_app_oper_log SET SERDEPROPERTIES('serialization.null.format' = ' ')
    '''
    hivesql.append(sql5)
    #**********************************
    #程序运行(生成用户平台级汇总日表(平台级):TB_BS_MD_USER_APP_INFO_DAY)
    #**********************************
    sql6='''
    create external table if not exists TB_BS_MD_USER_APP_INFO_DAY (
    DEL_DATE string,
    REG_ID string,
    REG_TYPE string,
    USER_ID string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    USER_NAME string,
    VISIT_COUNT INT,
    VISIT_DISIP_COUNT INT,
    IP_COUNT_FAMILY INT,
    IP_COUNT_CITY INT,
    IP_DISCOUNT_CITY INT,
    IP_COUNT_PRO INT,
    IP_DISCOUNT_PRO INT,
    IP_COUNT_COUNTRY INT,
    IP_DISCOUNT_COUNTRY INT,
    IP_COUNT_OTHER INT,
    IP_DISCOUNT_OTHER INT,
    VISIT_COUNT_TIME0 INT,
    VISIT_COUNT_TIME1 INT,
    VISIT_COUNT_TIME2 INT,
    VISIT_COUNT_TIME3 INT,
    VISIT_COUNT_WORK0 INT,
    VISIT_COUNT_WORK1 INT,
    VISIT_FLOWS_SUM INT,
    VISIT_TIMES_SUM INT,
    ADD_COUNT INT,
    DEL_COUNT INT,
    UPD_COUNT INT,
    QUERY_COUNT INT,
    OTHER_COUNT INT) 
    row format delimited 
    fields terminated by '%s' 
    location '/xdtrdata/ASMC/md/userApp'
    '''%(delimiter)
    hivesql.append(sql6)
    #**********************************
    #程序运行(生成用户平台级汇总月表(平台级):TB_BS_MD_USER_APP_INFO_MON)
    #**********************************
    sql7='''
    create external table if not exists TB_BS_MD_USER_APP_INFO_MON (
    DEL_MONTH string,
    REG_ID string,
    REG_TYPE string,
    USER_ID string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    USER_NAME string,
    VISIT_COUNT INT,
    VISIT_DISIP_COUNT INT,
    IP_COUNT_FAMILY INT,
    IP_COUNT_CITY INT,
    IP_DISCOUNT_CITY INT,
    IP_COUNT_PRO INT,
    IP_DISCOUNT_PRO INT,
    IP_COUNT_COUNTRY INT,
    IP_DISCOUNT_COUNTRY INT,
    IP_COUNT_OTHER INT,
    IP_DISCOUNT_OTHER INT,
    VISIT_COUNT_TIME0 INT,
    VISIT_COUNT_TIME1 INT,
    VISIT_COUNT_TIME2 INT,
    VISIT_COUNT_TIME3 INT,
    VISIT_COUNT_WORK0 INT,
    VISIT_COUNT_WORK1 INT,
    VISIT_FLOWS_SUM INT,
    VISIT_TIMES_SUM INT,
    ADD_COUNT INT,
    DEL_COUNT INT,
    UPD_COUNT INT,
    QUERY_COUNT INT,
    OTHER_COUNT INT) 
    row format delimited 
    fields terminated by '%s'
    location '/xdtrdata/ASMC/md/userAppMon'
    '''%(delimiter)
    hivesql.append(sql7)
    #**********************************
    #程序运行(用户模块级汇总日表(模块级):TB_BS_MD_USER_OPER_INFO_DAY)
    #**********************************
    sql8='''
    create external table if not exists TB_BS_MD_USER_OPER_INFO_DAY (
    DEL_DATE string,
    REG_ID string,
    REG_TYPE string,
    USER_ID string,
    OPERATE_NAME string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    USER_NAME string,
    VISIT_COUNT INT,
    VISIT_DISIP_COUNT INT,
    IP_COUNT_FAMILY INT,
    IP_COUNT_CITY INT,
    IP_DISCOUNT_CITY INT,
    IP_COUNT_PRO INT,
    IP_DISCOUNT_PRO INT,
    IP_COUNT_COUNTRY INT,
    IP_DISCOUNT_COUNTRY INT,
    IP_COUNT_OTHER INT,
    IP_DISCOUNT_OTHER INT,
    VISIT_COUNT_TIME0 INT,
    VISIT_COUNT_TIME1 INT,
    VISIT_COUNT_TIME2 INT,
    VISIT_COUNT_TIME3 INT,
    VISIT_COUNT_WORK0 INT,
    VISIT_COUNT_WORK1 INT,
    VISIT_FLOWS_SUM INT,
    VISIT_TIMES_SUM INT,
    ADD_COUNT INT,
    DEL_COUNT INT,
    UPD_COUNT INT,
    QUERY_COUNT INT,
    OTHER_COUNT INT) 
    row format delimited 
    fields terminated by '%s'
    location '/xdtrdata/ASMC/md/userOper'
    '''%(delimiter)
    hivesql.append(sql8)
    #**********************************
    #程序运行(生成用户模块级汇总月表(模块级):TB_BS_MD_USER_OPER_INFO_MON)
    #**********************************
    sql9='''
    create external table if not exists TB_BS_MD_USER_OPER_INFO_MON (
    DEL_MONTH string,
    REG_ID string,
    REG_TYPE string,
    USER_ID string,
    OPERATE_NAME string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    USER_NAME string,
    VISIT_COUNT INT,
    VISIT_DISIP_COUNT INT,
    IP_COUNT_FAMILY INT,
    IP_COUNT_CITY INT,
    IP_DISCOUNT_CITY INT,
    IP_COUNT_PRO INT,
    IP_DISCOUNT_PRO INT,
    IP_COUNT_COUNTRY INT,
    IP_DISCOUNT_COUNTRY INT,
    IP_COUNT_OTHER INT,
    IP_DISCOUNT_OTHER INT,
    VISIT_COUNT_TIME0 INT,
    VISIT_COUNT_TIME1 INT,
    VISIT_COUNT_TIME2 INT,
    VISIT_COUNT_TIME3 INT,
    VISIT_COUNT_WORK0 INT,
    VISIT_COUNT_WORK1 INT,
    VISIT_FLOWS_SUM INT,
    VISIT_TIMES_SUM INT,
    ADD_COUNT INT,
    DEL_COUNT INT,
    UPD_COUNT INT,
    QUERY_COUNT INT,
    OTHER_COUNT INT) 
    row format delimited 
    fields terminated by '%s'
    location '/xdtrdata/ASMC/md/userOperMon'
    '''%(delimiter)
    hivesql.append(sql9)
    #**********************************
    #程序运行(生成用户操作习惯画像汇总日表:TB_BS_RP_USER_OPER_PHOTO_DAY)
    #**********************************
    sql10='''
    create external table if not exists TB_BS_RP_USER_OPER_PHOTO_DAY (
    DEL_DATE string,
    USER_ID string,
    USER_NAME string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    PHOTO_IP string,
    PHOTO_IP_COUNT INT,
    PHOTO_POSITION string,
    PHOTO_POSITION_COUNT INT,
    PHOTO_WORK string,
    PHOTO_WORK_COUNT INT,
    PHOTO_TIME string,
    PHOTO_TIME_COUNT INT,
    PHOTO_TIMES string,
    PHOTO_TIMES_COUNT INT,
    PHOTO_FLOWS string,
    PHOTO_FLOWS_COUNT INT,
    PHOTO_LONG string,
    PHOTO_LONG_COUNT INT,
    PHOTO_APPS1 string,
    PHOTO_APPS2 string,
    PHOTO_APPS3 string,
    PHOTO_OPERATIONS string,
    PHOTO_OPERATIONS_COUNT INT,
    PHOTO_MODULAR1 string,
    PHOTO_MODULAR2 string,
    PHOTO_MODULAR3 string) 
    row format delimited 
    fields terminated by '%s'
    location '/xdtrdata/ASMC/md/userPhoto'
    '''%(delimiter)
    hivesql.append(sql10)
    #**********************************
    #程序运行(用户操作习惯画像汇总月表:TB_BS_RP_USER_OPER_PHOTO_MON)
    #**********************************
    sql11='''
    create external table if not exists TB_BS_RP_USER_OPER_PHOTO_MON (
    DEL_MONTH string,
    USER_ID string,
    USER_NAME string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    PHOTO_IP string,
    PHOTO_IP_COUNT INT,
    PHOTO_POSITION string,
    PHOTO_POSITION_COUNT INT,
    PHOTO_WORK string,
    PHOTO_WORK_COUNT INT,
    PHOTO_TIME string,
    PHOTO_TIME_COUNT INT,
    PHOTO_TIMES string,
    PHOTO_TIMES_COUNT INT,
    PHOTO_FLOWS string,
    PHOTO_FLOWS_COUNT INT,
    PHOTO_LONG string,
    PHOTO_LONG_COUNT INT,
    PHOTO_APPS1 string,
    PHOTO_APPS2 string,
    PHOTO_APPS3 string,
    PHOTO_OPERATIONS string,
    PHOTO_OPERATIONS_COUNT INT,
    PHOTO_MODULAR1 string,
    PHOTO_MODULAR2 string,
    PHOTO_MODULAR3 string) 
    row format delimited 
    fields terminated by '%s'
    location '/xdtrdata/ASMC/md/userPhotoMon'
    '''%(delimiter)
    hivesql.append(sql11)
    #**********************************
    #程序运行(生成用户风险度画像日表:TB_BS_RP_USER_RISK_PHOTO_DAY)
    #**********************************
    sql12='''
    create external table if not exists TB_BS_RP_USER_RISK_PHOTO_DAY (
    DEL_DATE string,
    USER_ID string,
    USER_NAME string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    RISK_SCORE INT,
    PHOTO_IP_SCORE INT,
    PHOTO_POSITION_SCORE INT,
    PHOTO_WORK_SCORE INT,
    PHOTO_TIME_SCORE INT,
    PHOTO_TIMES_COUNT INT,
    PHOTO_FLOWS_COUNT INT,
    PHOTO_OPERATIONS_SCORE INT,
    PHOTO_DB_SCORE INT) 
    row format delimited 
    fields terminated by '%s'
    location '/xdtrdata/ASMC/md/userRisk'
    '''%(delimiter)
    hivesql.append(sql12)
    #**********************************
    #程序运行(生成用户风险度画像月表:TB_BS_RP_USER_RISK_PHOTO_MON)
    #**********************************
    sql13='''
    create external table if not exists TB_BS_RP_USER_RISK_PHOTO_MON (
    DEL_MONTH string,
    USER_ID string,
    USER_NAME string,
    ORGANIZATION string,
    ORGANIZATION_ID string,
    RISK_SCORE INT,
    PHOTO_IP_SCORE INT,
    PHOTO_POSITION_SCORE INT,
    PHOTO_WORK_SCORE INT,
    PHOTO_TIME_SCORE INT,
    PHOTO_TIMES_COUNT INT,
    PHOTO_FLOWS_COUNT INT,
    PHOTO_OPERATIONS_SCORE INT,
    PHOTO_DB_SCORE INT) 
    row format delimited 
    fields terminated by '%s' 
    location '/xdtrdata/ASMC/md/userRiskMon'
    '''%(delimiter)
    hivesql.append(sql13)
    #**********************************
    #程序运行(生成一个平台id与平台名称对应的维表:TB_BS_DIC_REG)
    #**********************************
    sql14='''
    create table if not exists TB_BS_DIC_REG (
    REG_ID  string,
    AssetName  string)
    row format delimited 
    fields terminated by '#'
    '''
    hivesql.append(sql14)
    #**********************************
    #程序运行(生成一个画像分类ID与画像名称对应的维表:TB_BS_DIC_PHOTO)
    #**********************************
    sql15='''
    create table if not exists TB_BS_DIC_PHOTO (
    PHOTO_TYPE string,
    PHOTO_NAME string,
    PHOTO_TYPE_ID INT,
    PHOTO_TYPE_NAME string) 
    row format delimited 
    fields terminated by '#'
    '''
    hivesql.append(sql15)
    #**********************************
    #程序运行(生成风险度人数对应日表:TB_BS_RP_SYS_RISK_NUMBER_DAY)
    #**********************************
    sql16='''
    create table if not exists TB_BS_RP_SYS_RISK_NUMBER_DAY (
    DEL_DATE date,
    RISK_ID INT,
    RISK_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int,day int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql16)
    #**********************************
    #程序运行(生成风险度人数对应月表:TB_BS_RP_SYS_RISK_NUMBER_MON)
    #**********************************
    sql17='''
    create table if not exists TB_BS_RP_SYS_RISK_NUMBER_MON (
    DEL_MONTH string,
    RISK_ID INT,
    RISK_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql17)
    #**********************************
    #程序运行(生成平台人数对应日表:TB_BS_RP_PLAT_NUMBER_DAY)
    #**********************************
    sql18='''
    create table if not exists TB_BS_RP_PLAT_NUMBER_DAY (
    DEL_DATE date,
    REG_ID string,
    REG_TYPE string,
    REG_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int,day int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql18)
    #**********************************
    #程序运行(生成平台人数对应月表:TB_BS_RP_PLAT_NUMBER_MON)
    #**********************************
    sql19='''
    create table if not exists TB_BS_RP_PLAT_NUMBER_MON (
    DEL_MONTH string,
    REG_ID string,
    REG_TYPE string,
    REG_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql19)
    #**********************************
    #程序运行(生成模块人数对应日表:TB_BS_RP_MOD_NUMBER_DAY)
    #**********************************
    sql20='''
    create table if not exists TB_BS_RP_MOD_NUMBER_DAY (
    DEL_DATE date,
    REG_ID string,
    REG_NAME string,
    OPERATE_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int,day int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql20)
    #**********************************
    #程序运行(生成模块人数对应月表:TB_BS_RP_MOD_NUMBER_MON)
    #**********************************
    sql21='''
    create table if not exists TB_BS_RP_MOD_NUMBER_MON (
    DEL_MONTH string,
    REG_ID string,
    REG_NAME string,
    OPERATE_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql21)
    #**********************************
    #程序运行(生成用户画像对应人数日表:TB_BS_RP_SYS_PHOTO_DAY)
    #**********************************
    sql22='''
    create table if not exists TB_BS_RP_SYS_PHOTO_DAY (
    DEL_DATE date,
    PHOTO_TYPE string,
    PHOTO_TYPE_ID INT,
    PHOTO_TYPE_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int,day int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql22)
    #**********************************
    #程序运行(生成用户画像对应人数月表:TB_BS_RP_SYS_PHOTO_MON)
    #**********************************
    sql23='''
    create table if not exists TB_BS_RP_SYS_PHOTO_MON (
    DEL_MONTH string,
    PHOTO_TYPE string,
    PHOTO_TYPE_ID INT,
    PHOTO_TYPE_NAME string,
    USER_NUMBER INT) partitioned by (year int,mon int)
    row format delimited 
    fields terminated by '%s'
    '''%(delimiter)
    hivesql.append(sql23)
    #**********************************
    #程序运行(生成异常操作日志清单表:TB_BS_MD_APP_OPER_UNUSUAL_LOG)
    #**********************************
    sql24='''
    create table if not exists TB_BS_LS_APP_OPER_UNUSUAL_LOG(
    id int,
    Num_id string,
    Reg_id string,
    User_id string,
    Organization string,
    Organization_id string,
    User_name string,
    Operate_time string,
    ResponePackage int,
    Terminal_id string,
    Operate_type int,
    Operate_result int,
    Error_code string,
    Operate_name string,
    Operate_condition string,
    Insert_time string,
    Operate_date int,
    Index_time string,
    Update_time string) partitioned by (year int,mon int,day int)
    row format delimited
    fields terminated by '%s'
    ''' %(delimiter)
    hivesql.append(sql24)
    #**********************************
    #程序运行(替换异常操作日志清单表:TB_BS_MD_APP_OPER_UNUSUAL_LOG中的\N为'')
    #**********************************
    sql25='''
    alter table TB_BS_LS_APP_OPER_UNUSUAL_LOG set serdeproperties('serialization.null.format' = '')
    '''
    hivesql.append(sql25)
    #**********************************
    #程序运行(替换用户画像人数对应日表:TB_BS_RP_SYS_PHOTO_DAY中的\N为'')
    #**********************************
    sql26=r'''
    alter table TB_BS_RP_SYS_PHOTO_DAY set serdeproperties('serialization.null.format' = '0')
    '''
    hivesql.append(sql26)
    #**********************************
    #程序运行(替换用户画像人数对应月表:TB_BS_RP_SYS_PHOTO_DAY中的\N为'')
    #**********************************
    sql27=r'''
    alter table TB_BS_RP_SYS_PHOTO_MON set serdeproperties('serialization.null.format' = '0')
    '''
    hivesql.append(sql27)
    #运行hivesql
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e
