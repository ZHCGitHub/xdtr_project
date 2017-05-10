#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：Py_ASMC_Insert_TB_BS_RP_PLAT_NUMBER_Mon.py
# **  功能描述：根据输入的日表数据和两个维表关联查询，获取前台展示所需的数据                
# **         
# **              
# **         
# **  输入表：                    用户平台级汇总日表(平台级)：          TB_BS_MD_USER_APP_INFO_Mon
# **              
# **  维表：                        平台id与平台名称对应的维表：          TB_BS_DIC_REG
# **              
# **                 
# **  输出：                        平台人数对应日表：                                 TB_BS_RP_PLAT_NUMBER_Mon
# **              
# **
# **  创建者: 宋增旭
# **  电话:18236401973
# **  创建日期:%s24
# **  修改日志:
# **  修改日期:
# **  修改人 :
# **  修改内容:
# ** ---------------------------------------------------------------------------------------
# **  
# ** ---------------------------------------------------------------------------------------
# **  
# **  程序调用格式：python Py_ASMC_Insert_TB_BS_RP_PLAT_NUMBER_Mon.py
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
        date = datetime.datetime.strptime(arg1, "%Y%m").date()
        #today = datetime.date.today() #获得今天的日期   
#       yesterday = today - datetime.timedelta(days=30) #用今天日期减掉时间差，参数为1天，获得传入的日期
#       print("yesterday:",yesterday)
        if arg2=="year":
            year = date.strftime('%Y')
            return year
    
        if arg2=="month":
            month = date.strftime('%m')
            return month
    
    year = date(i,"year")
    month = date(i,"month")
    #获得传入的日期(%Y%m%d)
    date_month_str = "%s%s"%(year,month)
    #*************************************************************************************************************
    #SQL体，程序开始处理
    #*************************************************************************************************************
    hivesql = []
    #**********************************
    #程序运行(让用户平台级汇总日表(平台级)TB_BS_MD_USER_APP_INFO_Mon与
    #平台id与平台名称对应的维表:TB_BS_DIC_REG关联，
    #向结果表平台人数对应日表TB_BS_RP_PLAT_NUMBER_Mon中插入数据)
    #**********************************  
    sql=r'''
    insert overwrite table TB_BS_RP_PLAT_NUMBER_Mon partition(year=%s,mon=%s)
    SELECT DISTINCT
    a.DEL_MONTH,
    a.Reg_id,
    a.REG_TYPE,
    b.AssetName,
    a.USER_NUMBER
    FROM
    (
        SELECT
            REG_ID,
            1 AS REG_TYPE,
            count(DISTINCT user_id) AS user_number,
            '%s' AS DEL_MONTH
        FROM
            TB_BS_MD_USER_APP_INFO_Mon
        WHERE
            DEL_MONTH=%s
        GROUP BY
            REG_ID
    ) a
    LEFT JOIN TB_BS_DIC_REG b ON (a.REG_ID = b.REG_ID)
    '''%(year,month,date_month_str,date_month_str)
    hivesql.append(sql)
    
    #在Hive中运行sql语句
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e