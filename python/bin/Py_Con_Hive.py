#!/usr/bin/python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：py_con_hive.py
# **  功能描述：python连接HiveServer2的函数，向Hive里传递sql语句
# **  输入参数：sql语句
# **  配置文件：python-hive-site.xml
# **                  
# **  输出: 
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
# **  程序调用格式：HiveExe(sql)
# **    
#********************************************************************************************
# **  Copyright(c) 2016 , Inc. 
# **  All Rights Reserved.
#********************************************************************************************
import sys
import pyhs2
import os
import datetime
from Py_log import loggerFactory
from lxml import etree

#**********************************
#定义HiveExe函数
#**********************************
def HiveExe(hivesql):
    # 先读取XML文件中的配置数据
    # 由于config.xml放置在与当前文件相同的目录下，因此通过 __file__ 来获取XML文件的目录，然后再拼接成绝对路径
    # 这里利用了lxml库来解析XML
    root = etree.parse(os.path.join(os.path.dirname(__file__), 'python-hive-site.xml')).getroot()
    root1 = etree.parse(os.path.join(os.path.dirname(__file__), 'py-logs-site.xml')).getroot()
    #获取配置文件中的信息，并去除空格
    host = root.find('host').text.lstrip().rstrip()
    port = root.find('port').text.lstrip().rstrip()
    authMechanism = root.find('authMechanism').text.lstrip().rstrip()
    user = root.find('user').text.lstrip().rstrip()
    password = root.find('password').text.lstrip().rstrip()
    database = root.find('database').text.lstrip().rstrip()
    
    try:
        conn=pyhs2.connect(host='%s'%host,
                   port=port,
                   authMechanism="%s"%authMechanism,
                   user='%s'%user,
                   password='%s'%password,
                   database='%s'%database)
        cur=conn.cursor()
        #区分每次执行的日志
        loggerFactory.warning('**********************************************************************************************************************************')
        
        #将脚本开始时间打印到日志文件中
        starttime = datetime.datetime.now()
        loggerFactory.info('The python scripts start time is at '+datetime.datetime.strftime(starttime,"%Y-%m-%d %X"))
#         logtype = root1.find('logtype').text.lstrip().rstrip()
#         if logtype=='info':
#             #向日志文件中写入操作sql
#             loggerFactory.info('执行操作sql：%s'%sql)
#         elif logtype=='warning':
#             #向日志文件中写入操作sql
#             loggerFactory.warning('执行操作sql：%s'%sql)
#          
#         elif logtype=='error':
#             #向日志文件中写入操作sql
#             loggerFactory.error('执行操作sql：%s'%sql)
        for sql in hivesql:
            #运行sql语句
            loggerFactory.info('执行操作sql：%s'%sql)
            cur.execute(sql)            
        #向日志文件中写入sql操作成功标志
#         loggerFactory.info('执行sql操作成功')

        #将脚本结束时间打印到日志文件中
        endtime = datetime.datetime.now() 
        loggerFactory.info('The python scripts end time is at '+datetime.datetime.strftime(endtime,"%Y-%m-%d %X"))
        #将脚本运行时间打印到日志文件中
        #print (endtime - starttime).seconds
        
        loggerFactory.info('This python script running time is '+str((endtime - starttime).seconds)+' seconds')
    except Exception,e:
        loggerFactory.error('%s'%e)
        print '%s' %e
        