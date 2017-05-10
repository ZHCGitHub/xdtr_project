#! /bin/bash
#--------------------------------------------
# 这是一个使用sqoop导数据的脚本：
# 
# 功能：从mysql数据库向hdfs上导数据
# 特色：
#--------------------------------------------
##### 用户配置区 开始 #####
date=$1
date_time=${date:0:4}-${date:4:2}-${date:6:2}
today=$(date +%Y%m%d)
#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#读取config配置文件，将配置文件load进环境中，成为环境变量 
source ${shell_dir}/config 

log="${dir}shell_${today}.log"
#log="/opt/xdtr_project/ASMC/logs/shell_20120918"   #操作日志存放路径 
##### 用户配置区 结束  #####

#日志函数
#参数
    #参数一，级别，INFO ,WARN,ERROR
        #参数二，内容
#返回值
function shell_log()
{
    #判断参数的个数是否小于2
    #if [ 2 -gt $# ]
    #then
        #echo "parameter not right in zc_log function" ;
        #return ;
    #fi
    #判断日志文件log是否不存在,如果不存在则创建文件
    if [ ! -e "$log" ]
    then
        touch $log 
    fi
    
    #当前时间
    local curtime;
    curtime=`date +"%Y%m%d%H%M%S"`
    
    #设置当log日志溢出时日志存放名称
	curlog="${dir}""${curtime}.out"
    
    #获取文件字节大小
    local cursize;
    cursize=`cat $log | wc -c`;
	
    #判断cursize是否大于我们设定的文件大小fsize，如果为true的话，创建一个新的文件
    if [ $cursize -gt $fsize ]
    then
        mv $log $curlog
        touch $log ;
    fi    
    #写入文件
    #echo "$curtime $*" >> $log 2>&1;
   
    #exec 1>>$log    #在执行过程中将 stdout默认信息输出到日志文件中 
	#exec 2>>$log    #在执行过程将stderr错误信息输出到日志文件中 
	exec >>$log 2>&1 #将 stdout 和 stderr 合并后重定向到日志文件
} 


##### shell命令区 开始 #####
shell_log

#判断hdfs上是否存在目录F_SO_USER_OPER_LOG，如果存在则删除
hadoop fs -test -e /xdtrdata/ASMC/F_SO_USER_OPER_LOG
if [ $? -eq 0 ]
then
	hadoop fs -rm -r /xdtrdata/ASMC/F_SO_USER_OPER_LOG
fi


#使用sqoop从服务器ip地址：${mysql_ip}，表：${table_name} 导数据到hdfs上
sqoop import --connect jdbc:mysql://${mysql_source_ip}:3306/${mysql_source_database} --username ${mysql_source_user} --password ${mysql_source_password} --query "SELECT * FROM ${mysql_source_table} where \$CONDITIONS and operateTime BETWEEN '${date_time} 00:00:00' AND '${date_time} 23:59:59'" --split-by id -m ${map} --hive-drop-import-delims --fields-terminated-by "${delimiter}" --target-dir /xdtrdata/ASMC/F_SO_USER_OPER_LOG;
#sqoop import --connect jdbc:mysql://${mysql_ip}:3306/test --table ${table_name} --username root --password root123 -m 100 --hive-drop-import-delims --jar-file /opt/xdtr_project/ASMC/shell/${table_name}.jar --class-name tbl_log_operation --target-dir /xdtrdata/ASMC/F_SO_USER_OPER_LOG;
#--where "Operate_date=20131026"

#删除sqoop导数据成功的标志，方便数据向hive中load
hadoop fs -rm -r /xdtrdata/ASMC/F_SO_USER_OPER_LOG/_SUCCESS


