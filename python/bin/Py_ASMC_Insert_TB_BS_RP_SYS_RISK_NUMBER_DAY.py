#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：Py_ASMC_Insert_RP_Day.py
# **  功能描述：根据输入的日表数据和两个维表关联查询，获取前台展示所需的数据                
# **         
# **              
# **         
# **  输入表：                    用户风险度画像日表：                            TB_BS_RP_USER_RISK_PHOTO_DAY
# **                           
# **  维表：                        
# **                               
# **  输出：                        风险度人数对应日表：                             TB_BS_RP_SYS_RISK_NUMBER_DAY
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
# **  程序调用格式：python Py_ASMC_Insert_TB_BS_RP_SYS_RISK_NUMBER_DAY.py
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
#       date = today - datetime.timedelta(days=30) #用今天日期减掉时间差，参数为1天，获得传入的日期
#       print("date:",date)
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
    #程序运行(创建风险度人数临时表日表TB_BS_RP_TMP_RISK_NUMBER_DAY)
    #**********************************     
    sql1=r'''
    create table if not exists TB_BS_RP_TMP_RISK_NUMBER_DAY (
    RISK_ID int,
    RISK_NAME string,
    USER_NUMBER int,
    DEL_DATE date)
    row format delimited 
    fields terminated by '|'
    '''
    hivesql.append(sql1)
    #**********************************
    #程序运行(风险度人数临时日表TB_BS_RP_TMP_RISK_NUMBER_DAY插入数据)
    #**********************************  
    #插入无风险数据  
    sql2=r'''
    INSERT INTO TB_BS_RP_TMP_RISK_NUMBER_DAY
    SELECT
    0,
    "无风险",
    COUNT(DISTINCT User_id),
    '%s'
    FROM
    TB_BS_RP_USER_RISK_PHOTO_DAY
    WHERE
    DEL_DATE = %s
    AND RISK_SCORE < 60
    '''%(date,date_str)
    hivesql.append(sql2)
    
    #插入轻度风险数据  
    sql3=r'''
    INSERT INTO TB_BS_RP_TMP_RISK_NUMBER_DAY
    SELECT
    1,
    "轻度风险",
    COUNT(DISTINCT User_id),
    '%s'
    FROM
    TB_BS_RP_USER_RISK_PHOTO_DAY
    WHERE
    DEL_DATE = %s
    AND RISK_SCORE >= 60
    AND RISK_SCORE < 70
    '''%(date,date_str)
    hivesql.append(sql3)
    
    #插入中度风险数据  
    sql4=r'''
    INSERT INTO TB_BS_RP_TMP_RISK_NUMBER_DAY
    SELECT
    2,
    "中度风险",
    COUNT(DISTINCT User_id),
    '%s'
    FROM
    TB_BS_RP_USER_RISK_PHOTO_DAY
    WHERE
    DEL_DATE = %s
    AND RISK_SCORE >= 70
    AND RISK_SCORE < 90
    '''%(date,date_str)
    hivesql.append(sql4)
    
    #插入重度风险数据  
    sql5=r'''
    INSERT INTO TB_BS_RP_TMP_RISK_NUMBER_DAY
    SELECT
    3,
    "重度风险",
    COUNT(DISTINCT User_id),
    '%s'
    FROM
    TB_BS_RP_USER_RISK_PHOTO_DAY
    WHERE
    DEL_DATE = %s
    AND RISK_SCORE >= 90
    '''%(date,date_str)
    hivesql.append(sql5)
    
    #从临时表TB_BS_RP_TMP_RISK_NUMBER_DAY中将数据插入到风险度人数对应日表TB_BS_RP_SYS_RISK_NUMBER_DAY
    sql6=r'''
    insert overwrite table TB_BS_RP_SYS_RISK_NUMBER_DAY partition(year=%s,mon=%s,day=%s) 
    select 
    DEL_DATE,
    RISK_ID,
    RISK_NAME,
    USER_NUMBER
    FROM
    TB_BS_RP_TMP_RISK_NUMBER_DAY
    '''%(year,month,day)
    hivesql.append(sql6)
    
    #删除临时表TB_BS_RP_TMP_RISK_NUMBER_DAY
    sql7=r'''
    drop table TB_BS_RP_TMP_RISK_NUMBER_DAY
    '''
    hivesql.append(sql7)
    
    #在Hive中运行sql语句
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e