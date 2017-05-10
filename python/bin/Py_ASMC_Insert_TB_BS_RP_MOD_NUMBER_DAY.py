#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：Py_ASMC_Insert_TB_BS_RP_MOD_NUMBER_DAY.py
# **  功能描述：根据输入的日表数据和两个维表关联查询，获取前台展示所需的数据                
# **         
# **              
# **         
# **  输入表：                    用户模块级汇总日表(模块级)：          TB_BS_MD_USER_OPER_INFO_DAY
# **              
# **  维表：                        平台id与平台名称对应的维表：          TB_BS_DIC_REG
# **              
# **                 
# **  输出：                        模块人数对应日表：                                 TB_BS_RP_MOD_NUMBER_DAY 
# **              
# **
# **  创建者: 宋增旭
# **  电话:18236401973
# **  创建日期:20161124
# **  修改日志:
# **  修改日期:
# **  修改人 :
# **  修改内容:
# ** ---------------------------------------------------------------------------------------
# **  
# ** ---------------------------------------------------------------------------------------
# **  
# **  程序调用格式：python Py_ASMC_Insert_TB_BS_RP_MOD_NUMBER_DAY.py
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
#       yesterday = today - datetime.timedelta(days=30) #用今天日期减掉时间差，参数为1天，获得传入的日期
#       print("yesterday:",yesterday)
        if arg2=="year":
            year = date.strftime('%Y')
            return year
    
        if arg2=="month":
            month = date.strftime('%m')
            return month
    
        if arg2=="day":
            day = date.strftime('%d')
            return day
    
    year = date(i,"year")
    month = date(i,"month")
    day = date(i,"day")
    #获得传入的日期(%Y-%m-%d)
    date = "%s-%s-%s"%(year,month,day)
    #获得传入的日期(%Y%m%d)
    date_str = "%s%s%s"%(year,month,day)
    
    
    #*************************************************************************************************************
    #SQL体，程序开始处理
    #*************************************************************************************************************
    hivesql = []
    #**********************************
    #程序运行(用户模块级汇总日表(模块级)TB_BS_MD_USER_OPER_INFO_DAY与
    #平台id与平台名称对应的维表:TB_BS_DIC_REG关联，
    #向结果表模块人数对应日表TB_BS_RP_MOD_NUMBER_DAY中插入数据)
    #**********************************  
    sql=r'''
    insert overwrite table TB_BS_RP_MOD_NUMBER_DAY partition(year=%s,mon=%s,day=%s) 
    SELECT DISTINCT
    a.DEL_DATE,
    a.REG_ID,
    b.AssetName,
    a.OPERATE_NAME,
    a.USER_NUMBER
    FROM
        (
            SELECT
                REG_ID,
                OPERATE_NAME,
                count(DISTINCT user_id) AS user_number,
                '%s' AS DEL_DATE
            FROM
                TB_BS_MD_USER_OPER_INFO_DAY
            WHERE
                DEL_DATE=%s
            GROUP BY
                OPERATE_NAME,
                REG_ID
        ) a
    LEFT JOIN TB_BS_DIC_REG b ON (a.REG_ID = b.RegisteredId)
    '''%(year,month,day,date,date_str)
    hivesql.append(sql)
    #在Hive中运行sql语句
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e