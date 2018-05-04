# 根据邮件主题进行划分，将每一天的邮件分为三类，并存储为csv
import pandas as pd
import time

def classify(file_root):
	df = pd.read_csv(file_root+'/new.csv', encoding='gbk')
	data_string = df.astype(str)

	# class1_key = ['ALARM', 'RECOVER', 'api汇总', '测试脚本', '传输设置', '地图配置', '分析平台配置', '技术分享', '平台分析', '前后端接口', '软件开发文档', '特殊字段说明', '系统配置', '部署文档', '用户手册', '需求', '初验文档', '概要设计', '实施方案', '搜索详细设计', 'EmergencyDataBaseFatalError', '平台介绍', '终验文档', '项目']
	# frames_class1 = []
	# for key in class1_key:
	# 	data = data_string[data_string['subject'].str.contains(key)]
	# 	frames_class1.append(data)
	# frames_class1 = pd.concat(frames_class1)
	#
	# print(frames_class1)
	# frames_class1.to_csv(file_path+'01/email_dev.csv', encoding='gbk')

	data_dev = data_string[data_string['subject'].str.contains('ALARM|RECOVER|api汇总|测试脚本|传输设置|地图配置|分析平台配置|技术分享|平台分析|前后端接口|软件开发文档|特殊字段说明|系统配置|部署文档|用户手册|需求|初验文档|概要设计|实施方案|搜索详细设计|EmergencyDataBaseFatalError|平台介绍|终验文档|项目')]
	# print(data_dev)
	data_dev.to_csv(file_root+'/email_dev.csv', encoding='gbk')

	data_finance = data_string[data_string['subject'].str.contains('报销|财务|成本控制|互联网资产监控报警|会计核算|税务|资金')]
	data_finance.to_csv(file_root+'/email_finance.csv', encoding='gbk')

	data_hr = data_string[data_string['subject'].str.contains('zhaopin\.com|offer|复试通知|岗位说明|简历|个人资料|公司简介|录用通知|劳动合同|介绍|面试通知|培训邀请|人员档案|猎聘网|报名参加|新员工|考勤|绩效考核|职位推送|招聘信息|面试邀请')]
	data_hr.to_csv(file_root + '/email_hr.csv', encoding='gbk')


if __name__=='__main__':
	start = time.time()

	file_path = '../raw_data/2017-11-'
	num = list(range(10, 31))
	other_num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
	num.extend(other_num)

	for i in num:
		file_root = file_path+str(i)
		try:
			classify(file_root)
		except UnicodeDecodeError:
			print(i)
			continue


	end = time.time()
	print('cost time:'+str(end-start)+' s')