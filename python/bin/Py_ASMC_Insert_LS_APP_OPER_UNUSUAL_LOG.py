#!/usr/bin/python
# -*- coding: utf-8 -*-
# *******************************************************************************************
# **  文件名称：Py_ASMC_Insert_LS_APP_OPER_UNUSUAL_LOG.py
# **  功能描述：将中度汇总表与用户平台汇总月表关联(查询用户操作平台是否属于上月操作平台
# **              ，如果不是的话，将操作信息插入异常信息表)
# **  输入:TB_ASMC_LS_APP_OPER_LOG(原始日志表)   TB_BS_MD_USER_APP_INFO_MON(用户平台汇总月表)
# **  输出:TB_BS_LS_APP_OPER_UNUSUAL_LOG(用户异常操作信息表)
# **  创建者:宋增旭 
# **  电话:18236401973
# **  创建日期:20170301
# **  修改日志:
# **  修改日期:
# **  修改人 :
# **  修改内容:
# ** ---------------------------------------------------------------------------------------
# **  
# ** ---------------------------------------------------------------------------------------
# **  
# **  程序调用格式：python Py_ASMC_Insert_LS_APP_OPER_UNUSUAL_LOG.py 20161101
# **    
# ********************************************************************************************
# **  Copyright(c) 2016 , Inc. 
# **  All Rights Reserved.
# ********************************************************************************************
import sys
from Py_Con_Hive import HiveExe
from datetime import datetime

try:
    # **********************************
    # 获得传入日期
    # **********************************
    for i in sys.argv:
        print(i)

    def date(arg1, arg2):
        date = datetime.strptime(arg1, "%Y%m%d").date()

        if arg2 == "year":
            year = date.year
            return year

        if arg2 == "month":
            month = date.month
            return month

        if arg2 == "day":
            day = date.day
            return day


    def getLastDayOfLastMonth(arg1, arg2):
        year = arg1
        month = arg2

        if month == 1:
            month = 12
            year -= 1
        else:
            month -= 1
        return (datetime(year, month, 1)).strftime('%Y%m')

    year = date(i, "year")
    month = date(i, "month")
    day = date(i, "day")
    #获取指定月份的上一月
    month_last = getLastDayOfLastMonth(year,month)
    # *************************************************************************************************************
    # SQL体，程序开始处理
    # *************************************************************************************************************
    hivesql = []
    # **********************************
    # 程序运行(向中度汇总表tb_asmc_md_app_oper_log插入数据)
    # **********************************
    sql1 = r'''
    insert overwrite table TB_BS_LS_APP_OPER_UNUSUAL_LOG partition(year=%s,mon=%s,day=%s)
    SELECT
    a.id,
    a.Num_id,
    a.Reg_id,
    a.User_id,
    a.organization,
    a.organization_id,
    a.User_name,
    a.operate_time,
    a.responepackage,
    a.Terminal_id,
    a.Operate_type,
    a.Operate_result,
    a.Error_code,
    a.Operate_name,
    a.Operate_condition,
    a.insert_time,
    a.operate_date,
    a.index_time,
    a.update_time
    FROM
    tb_asmc_ls_app_oper_log AS a
    LEFT JOIN (
     SELECT DISTINCT user_id,user_name,reg_id,1 AS mark
     FROM
      tb_bs_md_user_app_info_mon
     WHERE
      del_month = %s
    ) AS b ON a.user_id = b.user_id
    AND a.user_name = b.user_name AND a.reg_id = b.reg_id WHERE b.mark IS NULL
    AND year=%s AND mon=%s AND day=%s
    ''' % (year,month,day,month_last,year,month,day)
    hivesql.append(sql1)
    # 在Hive中运行sql语句
    HiveExe(hivesql)
except Exception as e:
    print('%s' % e)


