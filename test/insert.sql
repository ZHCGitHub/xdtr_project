#向原始日志表中插入数据
INSERT INTO asmc_log_original_tb_businessosyslog (
    businessOSysLog_numId,
    businessSys_registeredId,
    businessOSysLog_userId,
    businessOSysLog_organization,
    businessOSysLog_organizationID,
    businessOSysLog_userName,
    businessOSysLog_operateTime,
    businessOSysLog_responePackage,
    businessOSysLog_terminalId,
    businessOSysLog_operateType,
    businessOSysLog_operateResult,
    businessOSysLog_errorCode,
    businessOSysLog_operateName,
    businessOSysLog_operatecndition,
    businessOSysLog_insertTime
) SELECT
	Num_id,
	Reg_id,
	User_id,
	Organization,
	Organization_id,
	User_name,
	CONCAT(
		"2017-01-04",
		" ",
		substring(Operate_time, 9, 2),
		":",
		substring(Operate_time, 11, 2),
		":",
		substring(Operate_time, 13, 2)
	) AS businessOSysLog_responePackage,
	FLOOR(1 +(RAND() * 10000)) AS businessOSysLog_responePackage,
	Terminal_id,
	Operate_type,
	Operate_result,
	Error_code,
	Operate_name,
	Operate_condition,
	CONCAT(
		"2017-01-04",
		" ",
		substring(Operate_time, 9, 2),
		":",
		substring(Operate_time, 11, 2),
		":",
		substring(Operate_time, 13, 2)
	) AS businessOSysLog_insertTime
FROM
	tbl_log_operation
LIMIT 20000000;