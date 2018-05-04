# 将员工按照邮件主题进行分类
import pandas as pd
from time import time
import re
from pandas.core.frame import DataFrame

def stat(filename):
	"""
	对于该邮件csv文件中的发件人和收件人进行统计
	:param filename: 已经分类好的csv文件
	:return: from_list, to_list
	"""
	df = pd.read_csv(filename, encoding='gbk')
	from_list = df['from']
	to_list = df['to']

	return from_list, to_list


def extract_id(list, flag=0):
	"""
	根据提供的set，提取其中的id
	:return: id_list
	"""
	id = []
	pattern = re.compile(r'(\d{4})@hightech\.com')

	# 考虑到收件人存在多个的情况，需要进行切分
	if flag == 1:
		for i in list:
			sub_list = i.split(';')
			for j in sub_list:
				re_result = re.search(pattern, j)
				try:
					id.append(re_result.group(1))
				except:
					continue

	else:
		for i in list:
			re_result = re.search(pattern, i)
			try:
				# print(re_result.group(1))
				id.append(re_result.group(1))
			except:
				continue

	return id


def dev_count(filename):

	from_list, to_list = stat(filename)

	id_from_list = list(set(extract_id(from_list)))
	id_to_list = list(set(extract_id(to_list, flag=1)))

	return id_from_list, id_to_list


def hr_count(filename):
	df = pd.read_csv(filename, encoding='gbk')
	data_string = df.astype(str)

	data_personal = data_string[data_string['subject'].str.contains('简历|个人资料')]
	data_staff = data_string[data_string['subject'].str.contains('复试通知|岗位说明|录用通知|面试通知|考勤')]

	from_personal_list = data_personal['from']
	to_personal_list = data_personal['to']

	from_staff_list = data_staff['from']
	to_staff_list = data_staff['to']

	id_personal_from_list = list(set(extract_id(from_personal_list)))
	id_personal_to_list = list(set(extract_id(to_personal_list, flag=1)))

	id_staff_from_list = list(set(extract_id(from_staff_list)))
	id_staff_to_list = list(set(extract_id(to_staff_list, flag=1)))

	id_personal_from_list.sort()
	id_personal_to_list.sort()

	id_staff_from_list.sort()
	id_staff_to_list.sort()

	return id_personal_from_list, id_personal_to_list, id_staff_from_list, id_staff_to_list


def finance_count(filename):
	df = pd.read_csv(filename, encoding='gbk')
	data_string = df.astype(str)

	data_from = data_string[data_string['subject'].str.contains('成本控制')]
	data_to = data_string[data_string['subject'].str.contains('成本控制')]

	from_list = data_from['from']
	id_from_list = list(set(extract_id(from_list)))

	to_list = data_to['to']
	id_to_list = list(set(extract_id(to_list, flag=1)))

	id_from_list.sort()
	id_to_list.sort()

	return id_from_list, id_to_list


def main(class_choose, sub_class_choose=None):
	file_path = '../raw_data/2017-11-'
	num = list(range(10, 31))
	other_num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
	num.extend(other_num)

	id_from_list = []
	id_to_list = []

	if class_choose == 'dev':
		for i in num:
			file_root = file_path + str(i)
			filename = file_root + '/email_dev.csv'
			a, b = dev_count(filename)
			id_from_list.extend(a)
			id_to_list.extend(b)

	elif class_choose == 'hr':
		for i in num:
			file_root = file_path + str(i)
			filename = file_root + '/email_hr.csv'
			id_personal_from_list, id_personal_to_list, id_staff_from_list, id_staff_to_list = hr_count(filename)

			if sub_class_choose == 'personal':
				id_from_list.extend(id_personal_from_list)
				id_to_list.extend(id_personal_to_list)
			elif sub_class_choose == 'staff':
				id_from_list.extend(id_staff_from_list)
				id_to_list.extend(id_staff_to_list)
			else:
				print('Input Error')
				exit(1)

	elif class_choose == 'finance':
		for i in num:
			file_root = file_path + str(i)
			filename = file_root + '/email_finance.csv'
			id_from_list_l, id_to_list_l = finance_count(filename)
			id_from_list.extend(id_from_list_l)
			id_to_list.extend(id_to_list_l)

			# if sub_class_choose == 'personal':
			# 	id_from_list.extend(id_personal_from_list)
			# 	id_to_list.extend(id_personal_to_list)
			# elif sub_class_choose == 'staff':
			# 	id_from_list.extend(id_staff_from_list)
			# 	id_to_list.extend(id_staff_to_list)
			# else:
			# 	print('Input Error')
			# 	exit(1)

	else:
		print('Input Error')
		exit(1)

	id_from_list = list(set(id_from_list))
	id_to_list = list(set(id_to_list))

	id_from_list.sort()
	id_to_list.sort()

	return id_from_list, id_to_list



if __name__ == '__main__':
	start_time = time()

	id_from_list, id_to_list = main('hr', sub_class_choose='staff')

	print(id_from_list)
	print(len(id_from_list))
	print(id_to_list)
	print(len(id_to_list))

	# list_1 = [i for i in id_to_list if i not in id_from_list]
	# print(list_1)

	data = DataFrame({'hr_personal': id_from_list})
	data.to_csv('id_hr.csv')

	# data = list(set((id_from_list+id_to_list)))
	# data = DataFrame({'finance': data})
	# data.to_csv('id_finance.csv')

	end_time = time()
	print('cost time: '+str(end_time-start_time)+' s')