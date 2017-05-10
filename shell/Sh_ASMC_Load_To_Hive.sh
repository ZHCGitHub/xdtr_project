#! /bin/bash
#--------------------------------------------
# 这是向hive中load数据与生成补全信息表的脚本：
# 
# 功能：向hive中load数据；生成补全信息表；向异常信息表插入数据
# 特色：
#--------------------------------------------
##### 用户配置区 开始 #####
#获取时间
date=$1

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

#echo "*************************Running: Py_ASMC_Load_LS_APP_OPER_LOG.py*************************" >> $log
python ${xdtr_ASMC_python_dir}/Py_ASMC_Load_LS_APP_OPER_LOG.py ${date}

#echo "*************************Running: test.py*************************" >> $log
python ${xdtr_ASMC_python_dir}/Py_ASMC_Insert_MD_APP_OPER_LOG.py ${date}

#echo "*************************Running: Sh_ASMC_Put_Success.sh*************************" >> $log
sh ${shell_dir}/Sh_ASMC_Put_Success.sh ${date}

###############################向异常信息表插入数据#################################
python ${xdtr_ASMC_python_dir}/Py_ASMC_Insert_LS_APP_OPER_UNUSUAL_LOG.py ${date}
##### shell命令区  结束  #####
