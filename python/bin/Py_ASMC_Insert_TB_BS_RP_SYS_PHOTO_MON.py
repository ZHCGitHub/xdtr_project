#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：Py_ASMC_Insert_TB_BS_RP_SYS_PHOTO_Mon.py
# **  功能描述：根据输入的日表数据和两个维表关联查询，获取前台展示所需的数据                
# **         
# **              
# **         
# **  输入表：                    用户平台级汇总日表(平台级)：          TB_BS_MD_USER_APP_INFO_Mon
# **              用户模块级汇总日表(模块级)：          TB_BS_MD_USER_OPER_INFO_Mon
# **              用户操作习惯画像汇总日表：                TB_BS_RP_USER_OPER_PHOTO_Mon
# **              用户风险度画像日表：                            TB_BS_RP_USER_RISK_PHOTO_Mon
# **  维表：                        平台id与平台名称对应的维表：          TB_BS_DIC_REG
# **              画像分类ID与画像名称对应的维表: TB_BS_DIC_PHOTO
# **                 
# **  输出：                        风险度人数对应日表：                             TB_BS_RP_SYS_RISK_NUMBER_Mon
# **              平台人数对应日表：                                 TB_BS_RP_PLAT_NUMBER_Mon
# **              模块人数对应日表：                                 TB_BS_RP_MOD_NUMBER_Mon 
# **              用户画像对应人数日表：                         TB_BS_RP_SYS_PHOTO_Mon
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
# **  程序调用格式：python Py_ASMC_Insert_TB_BS_RP_SYS_PHOTO_Mon.py
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
    #程序运行(创建用户画像对应人数临时表日表TB_BS_TMP_SYS_PHOTO_MON)
    #**********************************  
    sql1=r'''
    create table if not exists TB_BS_TMP_SYS_PHOTO_MON (
    PHOTO_TYPE string,
    PHOTO_TYPE_ID int,
    USER_NUMBER int,
    DEL_MONTH string)
    row format delimited 
    fields terminated by ''
    '''
    hivesql.append(sql1)
    #**********************************
    #程序运行(用户画像对应人数临时日表TB_BS_TMP_SYS_PHOTO_MON插入数据)
    #**********************************  
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入ip画像的数据
    sql2=r'''
    insert into table TB_BS_TMP_SYS_PHOTO_MON
        SELECT
        "111",
        PHOTO_IP,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_IP
    '''%(date_month_str,date_month_str)
    hivesql.append(sql2)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入用户办公位置画像的数据
    sql3=r'''
    insert into table TB_BS_TMP_SYS_PHOTO_MON
    SELECT
        "121",
        PHOTO_POSITION,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_POSITION
    '''%(date_month_str,date_month_str)
    hivesql.append(sql3)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入用户工作时间画像的数据
    sql4=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_MON
    SELECT
        "211",
        PHOTO_WORK,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_WORK
    '''%(date_month_str,date_month_str)
    hivesql.append(sql4)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入用户工作时段画像的数据 
    sql5=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_MON
    SELECT
        "212",
        PHOTO_TIME,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_TIME
    '''%(date_month_str,date_month_str)
    hivesql.append(sql5)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入用户访问次数画像的数据 
    sql6=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_MON
    SELECT
        "221",
        PHOTO_TIMES,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_TIMES
    '''%(date_month_str,date_month_str)
    hivesql.append(sql6)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入用户访问流量画像的数据
    sql7=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_MON
    SELECT
        "231",
        PHOTO_FLOWS,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_FLOWS
    '''%(date_month_str,date_month_str)
    hivesql.append(sql7)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入用户访问时长画像的数据  
    sql8=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_MON
    SELECT
        "241",
        PHOTO_LONG,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_LONG
    '''%(date_month_str,date_month_str)
    hivesql.append(sql8)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_MON中插入用户操作习惯画像的数据  
    sql9=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_MON
    SELECT
        "321",
        PHOTO_OPERATIONS,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_Mon
    WHERE
        DEL_MONTH=%s
    GROUP BY
        PHOTO_OPERATIONS
    '''%(date_month_str,date_month_str)
    hivesql.append(sql9)
    
    #让临时表TB_BS_TMP_SYS_PHOTO_MON与画像维表关联，并将数据导入到结果表TB_BS_RP_SYS_PHOTO_Mon中
    sql19=r'''
    insert overwrite table TB_BS_RP_SYS_PHOTO_MON partition(year=%s,mon=%s)
    SELECT
        concat('%s''%s'),
        a.PHOTO_NAME,
        a.PHOTO_TYPE_ID,
        a.PHOTO_TYPE_NAME,
        b.USER_NUMBER
    FROM
        TB_BS_DIC_PHOTO a
    LEFT JOIN TB_BS_TMP_SYS_PHOTO_MON b
    ON
        a.PHOTO_TYPE = b.PHOTO_TYPE
    AND a.PHOTO_TYPE_ID = b.PHOTO_TYPE_ID
    '''%(year,month,year,month)
    hivesql.append(sql19)
    
    #删除临时表  TB_BS_TMP_SYS_PHOTO_MON
    sql10=r'''
    DROP TABLE TB_BS_TMP_SYS_PHOTO_MON
    '''
    hivesql.append(sql10)
    #在Hive中运行sql语句
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e