#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：Py_ASMC_Insert_APP_OPER_LOG.py
# **  功能描述：将            原始日志表：          tb_asmc_ls_app_oper_log
# **              ip维表：                tb_asmc_dic_ip_area_code
# **              时间维表：              tb_asmc_dic_time_catalog
# **         三表关联查询，并将结果插入到
# **              中度汇总表：          tb_asmc_md_app_oper_log
# **         
# **  输入：tb_asmc_ls_app_oper_log(原始日志表)    tb_asmc_dic_ip_area_code(ip维表)   tb_asmc_dic_time_catalog(时间维表) 
# **                  
# **  输出:tb_asmc_md_app_oper_log(中度汇总表)
# **  创建者:宋增旭 
# **  电话:18236401973
# **  创建日期:20161125
# **  修改日志:
# **  修改日期:20161125
# **  修改人 :宋增旭
# **  修改内容:将单位id和单位名称致NULL，在insert中加入一个user_id!=000000000000000000的条件
# ** ---------------------------------------------------------------------------------------
# **  
# ** ---------------------------------------------------------------------------------------
# **  
# **  程序调用格式：python test.py
# **    
#********************************************************************************************
# **  Copyright(c) 2016 , Inc. 
# **  All Rights Reserved.
#********************************************************************************************
import sys
import datetime
from Py_Con_Hive import HiveExe
try:
    #**********************************
    #获得传入日期
    #**********************************
    for i in sys.argv:
        print(i)
    
    def date(arg1,arg2):    
        date = datetime.datetime.strptime(arg1, "%Y%m%d").date()
        #today = datetime.date.today() #获得今天的日期   
#       yesterday = today - datetime.timedelta(days=30) #用今天日期减掉时间差，参数为1天，获得昨天的日期
#       print("yesterday:",yesterday)
        if arg2=="year":
            year = date.year
            return year
    
        if arg2=="month":
            month = date.month
            return month
    
        if arg2=="day":
            day = date.day
            return day
    
    year = date(i,"year")
    month = date(i,"month")
    day = date(i,"day")
       
#     today = datetime.date.today() #获得今天的日期
#     yesterday = today - datetime.timedelta(days=1) #用今天日期减掉时间差，参数为1天，获得昨天的日期
#     #获取昨天的年月日
#     year = yesterday.year
#     month = yesterday.month
#     day = yesterday.day
    #*************************************************************************************************************
    #SQL体，程序开始处理
    #*************************************************************************************************************
    hivesql = []
    #**********************************
    #程序运行(向中度汇总表tb_asmc_md_app_oper_log插入数据)
    #**********************************    
    sql1=r'''
    insert overwrite table TB_ASMC_MD_APP_OPER_LOG partition(year=%s,mon=%s,day=%s) 
    select 
    a.id,
    a.Num_id,
    a.Reg_id,
    a.User_id,
    NULL,
    NULL,
    a.User_name,
    datetime_to_str(a.Operate_time),
    a.Terminal_id,
    a.Operate_type,
    a.Operate_result,
    a.Error_code,
    a.Operate_name,
    a.Operate_condition,
    datetime_to_str(a.Insert_time),
    b.Area_code,
    b.Area_name,
    b.Prov_name,
    b.Type_Provence,
    c.Flag_work,
    c.Flag_time,
    a.ResponePackage    
    From(select * from TB_ASMC_LS_APP_OPER_LOG where year=%s AND mon=%s AND day=%s) a
    left outer join TB_ASMC_DIC_IP_AREA_CODE b
    on
    (split(a.Terminal_id,'\\.')[0]=b.Ip_First and
    split(a.Terminal_id,'\\.')[1]=b.Ip_Second and
    split(a.Terminal_id,'\\.')[2]=b.Ip_Third)
    left outer Join TB_ASMC_DIC_TIME_CATALOG c
    on
    (substring(a.Operate_time,12,2)=c.Time_Hour and
    substring(a.Operate_time,15,2)=c.Time_Minute and
    isweek(a.Operate_time)=c.Flag_week)
    '''%(year,month,day,year,month,day)
    hivesql.append(sql1)
    #在Hive中运行sql语句
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e