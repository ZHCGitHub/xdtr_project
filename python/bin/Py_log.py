#!/usr/bin/python
# -*- coding: utf-8 -*-

from lxml import etree
import logging.handlers
import logging
import os
import sys
import time

# 提供日志功能
class loggerFactory:
    # 先读取XML文件中的配置数据
    # 由于config.xml放置在与当前文件相同的目录下，因此通过 __file__ 来获取XML文件的目录，然后再拼接成绝对路径
    # 这里利用了lxml库来解析XML
    root = etree.parse(os.path.join(os.path.dirname(__file__), 'py-logs-site.xml')).getroot()
    # 读取日志文件保存路径，并去除空格
    logpath = root.find('logpath').text.lstrip().rstrip()
    # 读取日志文件容量，并去除空格转换为字节
    logsize = 1024*1024*int(root.find('logsize').text.lstrip().rstrip())
    # 读取日志文件保存个数，并去除空格
    lognum = int(root.find('lognum').text.lstrip().rstrip())
    
    #**********************************
    #获得当前时间
    #**********************************    
    date=time.strftime('%Y%m%d',time.localtime(time.time()))
    #*************************************************************************************************************

    # 日志文件名：由用例脚本的名称，结合日志保存路径，得到日志文件的绝对路径
    #获取脚本名称
    pythonName=sys.argv[0].split('/')[-1].split('.')[0]
    logname='python_'+'%s'%date+'.log'
    logdir = os.path.join(logpath, logname)

    # 初始化logger
    log = logging.getLogger()
    # 日志格式，可以根据需要设置
    #fmt = logging.Formatter('[%(asctime)s][%(filename)s][line:%(lineno)d][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
    fmt = logging.Formatter('[%(asctime)s]'+'[%s]'%pythonName+'[%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

    # 日志输出到文件，这里用到了上面获取的日志名称，大小，保存个数
    handle1 = logging.handlers.RotatingFileHandler(logdir, maxBytes=logsize, backupCount=lognum)
    handle1.setFormatter(fmt)
    # 同时输出到屏幕，便于实施观察
    handle2 = logging.StreamHandler(sys.stdout)
    handle2.setFormatter(fmt)
    log.addHandler(handle1)
    log.addHandler(handle2)

    # 设置日志基本，这里设置为INFO，表示只有INFO级别及以上的会打印
    log.setLevel(logging.INFO)

    # 日志接口，用户只需调用这里的接口即可，这里只定位了INFO, WARNING, ERROR三个级别的日志，可根据需要定义更多接口
    @classmethod
    def info(cls, msg):
        cls.log.info(msg)
        return
  
    @classmethod
    def warning(cls, msg):
        cls.log.warning(msg)
        return
  
    @classmethod
    def error(cls, msg):
        cls.log.error(msg)
        return