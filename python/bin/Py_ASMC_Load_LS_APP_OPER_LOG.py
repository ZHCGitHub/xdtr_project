#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：Py_ASMC_Load_LS_APP_OPER_LOG.py
# **  功能描述：将hdfs上的/xdtrdata/ASMC/F_SO_USER_OPER_LOG/下所有文件load
# **         进hive的外部表tb_asmc_ls_app_oper_log中，分区为当前日期。
# **  输入：/xdtrdata/ASMC/F_SO_USER_OPER_LOG/所有文件    文件描述：不同字段用"#"分割
# **                  
# **  输出:tb_asmc_ls_app_oper_log表    表描述：外部表,分区为当前日期,例如：dt=20160918
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
# **  程序调用格式：python Py_ASMC_Load_LS_APP_OPER_LOG.py
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
    #程序开始运行，生成数据
    #**********************************     
    sql1='''
    LOAD DATA INPATH '/xdtrdata/ASMC/F_SO_USER_OPER_LOG/' OVERWRITE INTO TABLE tb_asmc_ls_app_oper_log partition(year=%s,mon=%s,day=%s)
    '''%(year,month,day)
    hivesql.append(sql1)
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e