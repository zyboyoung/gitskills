import pandas as pd
import os
import time

root_path = '../raw_data'
file_path = []
for root, dirs, files in os.walk(root_path):
	file_path.append(root)

file_path.remove(root_path)

def cal_frequency(subject):
	count = 0
	for file in file_path:
		file_name = file + '\\new.csv'
		df = pd.read_csv(file_name, encoding='gbk')
		df_string = df.astype(str)
		if subject=='系统预警':
			pattern = 'ALARM|RECOVER|EmergencyData|互联网资产监控报警'
		elif subject=='设计':
			pattern = '设计'
		elif subject=='文档':
			pattern = '文档'
		elif subject=='配置':
			pattern = '配置|传输设置'
		elif subject=='使用说明':
			pattern = '平台介绍|实施方案|特殊字段说明|用户手册|平台分析'
		elif subject=='其他':
			pattern = '测试脚本|api汇总'
		elif subject=='项目':
			pattern = '项目'
		elif subject=='需求':
			pattern = '需求'
		elif subject=='技术':
			pattern = '技术分享及探讨|技术分享安排'
		elif subject=='简历':
			pattern = '简历'
		elif subject=='通知':
			pattern = '通知'
		elif subject=='考勤':
			pattern = '考勤'
		elif subject=='岗位说明':
			pattern = '岗位说明书'
		elif subject=='资料':
			pattern = '资料'
		elif subject=='财务分析':
			pattern = '财务分析'
		elif subject=='财务':
			pattern = '^财务$'
		elif subject=='会计核算':
			pattern = '会计核算'

		count += len(df_string[df_string['subject'].str.contains(pattern)])

	return count



if __name__=='__main__':
	start_time = time.time()

	frequency = {}
	for subject in ['系统预警', '设计', '文档', '配置', '使用说明', '其他', '项目', '需求', '技术', '简历', '通知', '考勤', '岗位说明', '资料', '财务分析', '财务', '会计核算']:
		frequency[subject] = cal_frequency(subject)

	print(frequency)
	end_time = time.time()

	print('本次运行耗费时间：'+str(end_time-start_time)+' s')