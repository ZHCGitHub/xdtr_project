#!/usr/bin/python
# -*- coding: utf-8 -*-
#*******************************************************************************************
# **  文件名称：Py_ASMC_Insert_TB_BS_RP_SYS_PHOTO_DAY.py
# **  功能描述：根据输入的日表数据和两个维表关联查询，获取前台展示所需的数据                
# **         
# **              
# **         
# **  输入表：                    用户操作习惯画像汇总日表：                TB_BS_RP_USER_OPER_PHOTO_DAY
# **              
# **  维表：                        画像分类ID与画像名称对应的维表: TB_BS_DIC_PHOTO
# **                 
# **  输出：                        用户画像对应人数日表：                         TB_BS_RP_SYS_PHOTO_DAY
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
# **  程序调用格式：python Py_ASMC_Insert_TB_BS_RP_SYS_PHOTO_DAY.py
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
    #程序运行(创建用户画像对应人数临时表日表TB_BS_TMP_SYS_PHOTO_DAY)
    #**********************************  
    sql1=r'''
    create table if not exists TB_BS_TMP_SYS_PHOTO_DAY (
    PHOTO_TYPE string,
    PHOTO_TYPE_ID int,
    USER_NUMBER int,
    DEL_DATE date)
    row format delimited 
    fields terminated by '#'
    '''
    hivesql.append(sql1)
    #**********************************
    #程序运行(用户画像对应人数临时日表TB_BS_TMP_SYS_PHOTO_DAY插入数据)
    #**********************************  
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入ip画像的数据
    sql2=r'''
    insert into table TB_BS_TMP_SYS_PHOTO_DAY
        SELECT
        "111",
        PHOTO_IP,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_IP
    '''%(date,date_str)
    hivesql.append(sql2)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入用户办公位置画像的数据
    sql3=r'''
    insert into table TB_BS_TMP_SYS_PHOTO_DAY
    SELECT
        "121",
        PHOTO_POSITION,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_POSITION
    '''%(date,date_str)
    hivesql.append(sql3)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入用户工作时间画像的数据
    sql4=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_DAY
    SELECT
        "211",
        PHOTO_WORK,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_WORK
    '''%(date,date_str)
    hivesql.append(sql4)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入用户工作时段画像的数据 
    sql5=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_DAY
    SELECT
        "212",
        PHOTO_TIME,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_TIME
    '''%(date,date_str)
    hivesql.append(sql5)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入用户访问次数画像的数据 
    sql6=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_DAY
    SELECT
        "221",
        PHOTO_TIMES,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_TIMES
    '''%(date,date_str)
    hivesql.append(sql6)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入用户访问流量画像的数据
    sql7=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_DAY
    SELECT
        "231",
        PHOTO_FLOWS,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_FLOWS
    '''%(date,date_str)
    hivesql.append(sql7)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入用户访问时长画像的数据  
    sql8=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_DAY
    SELECT
        "241",
        PHOTO_LONG,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_LONG
    '''%(date,date_str)
    hivesql.append(sql8)
    
    #向临时表TB_BS_TMP_SYS_PHOTO_DAY中插入用户操作习惯画像的数据  
    sql9=r'''
    INSERT INTO TB_BS_TMP_SYS_PHOTO_DAY
    SELECT
        "321",
        PHOTO_OPERATIONS,
        COUNT(DISTINCT USER_ID),
        '%s'
    FROM
        TB_BS_RP_USER_OPER_PHOTO_DAY
    WHERE
        DEL_DATE=%s
    GROUP BY
        PHOTO_OPERATIONS
    '''%(date,date_str)
    hivesql.append(sql9)
    
    #让临时表TB_BS_TMP_SYS_PHOTO_DAY与画像维表关联，并将数据导入到结果表TB_BS_RP_SYS_PHOTO_DAY中
    sql10=r'''
    insert overwrite table TB_BS_RP_SYS_PHOTO_DAY partition(year=%s,mon=%s,day=%s)
    SELECT
        concat('%s','-','%s','-','%s'),
        a.PHOTO_NAME,
        a.PHOTO_TYPE_ID,
        a.PHOTO_TYPE_NAME,
        b.USER_NUMBER
    FROM
        TB_BS_DIC_PHOTO a
    LEFT JOIN TB_BS_TMP_SYS_PHOTO_DAY b
    ON
        a.PHOTO_TYPE = b.PHOTO_TYPE
    AND a.PHOTO_TYPE_ID = b.PHOTO_TYPE_ID
    '''%(year,month,day,year,month,day)
    hivesql.append(sql10)
    
    #删除临时表  TB_BS_TMP_SYS_PHOTO_DAY
    sql11=r'''
    DROP TABLE TB_BS_TMP_SYS_PHOTO_DAY
    '''
    hivesql.append(sql11)

    #在Hive中运行sql语句
    HiveExe(hivesql)
except Exception,e:
    print '%s' %e