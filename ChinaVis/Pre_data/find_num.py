# 找出统计时遗漏的ID
import pandas as pd
import time
from pandas.core.frame import DataFrame

def get_id_all():
	df = pd.read_csv('../raw_data/2017-11-01/checking.csv')
	id_all_1 = []
	for i in df['id']:
		id_all_1.append(i)

	id_all_1 = list(set(id_all_1))
	id_all_1.sort()
	return id_all_1

def get_id_csv():
	df_dev = pd.read_csv('id_dev.csv')
	df_finance = pd.read_csv('id_finance.csv')
	df_hr = pd.read_csv('id_hr.csv')

	id_all = []
	for i in df_dev['dev']:
		id_all.append(i)
	for i in df_hr['hr']:
		id_all.append(i)
	for i in df_finance['finance']:
		id_all.append(i)

	id_all = list(set(id_all))
	id_all.sort()

	return id_all


def find_email():
	file_path = '../raw_data/2017-11-'
	num = list(range(10, 31))
	other_num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
	num.extend(other_num)

	df_from_origin = pd.DataFrame(columns=['time', 'proto', 'sip', 'sport', 'dip', 'dport', 'from', 'to', 'subject'])
	df_to_origin = pd.DataFrame(columns=['time', 'proto', 'sip', 'sport', 'dip', 'dport', 'from', 'to', 'subject'])

	for i in num:
		df = pd.read_csv(file_path + str(i) + '/new.csv', encoding='gbk')
		a = df[df['from'].str.contains('1067@hightech\.com')]
		df_from_origin = pd.concat([df_from_origin, a])

		b = df[df['to'].str.contains('1067@hightech\.com')]
		df_to_origin = pd.concat([df_to_origin, b])

	return df_from_origin, df_to_origin

# 找出所有leader的收发邮件
def leader_find_email():
	file_path = '../raw_data/2017-11-'
	other_num = list(range(10, 31))
	num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
	num.extend(other_num)

	pd.set_option('display.width', 1000)



	for id in [1067, 1059, 1068, 1007, 1041, 1013]:
		df_from_origin = pd.DataFrame(columns=['from', 'to', 'subject'])
		df_to_origin = pd.DataFrame(columns=['from', 'to', 'subject'])
		for i in num:
			df = pd.read_csv(file_path + str(i) + '/new.csv', encoding='gbk')
			a = df[df['from'].str.contains(str(id) + '@hightech\.com')].drop(
				['time', 'proto', 'sip', 'sport', 'dip', 'dport'], axis=1)
			df_from_origin = pd.concat([df_from_origin, a])

			b = df[df['to'].str.contains(str(id) + '@hightech\.com')].drop(
				['time', 'proto', 'sip', 'sport', 'dip', 'dport'], axis=1)
			df_to_origin = pd.concat([df_to_origin, b])

		df_from_origin = df_from_origin.drop_duplicates()
		df_to_origin = df_to_origin.drop_duplicates()

		df_from = df_from_origin['subject'].drop_duplicates()
		df_to = df_to_origin['subject'].drop_duplicates()

		print('------' + str(id) + '------')
		print('------from------')
		print(list(df_from))
		print('\n')
		print('------to------')
		print(list(df_to))
		print('\n')


if __name__=='__main__':
	start_time = time.time()

	# id_all = get_id_all()
	# print(len(id_all))
    #
	# id_csv = get_id_csv()
	# print(len(id_csv))
    #
	# id_rest = [j for j in id_all if j not in id_csv]
	# print(id_rest)

	# num = range(10, 31)
	# for day in num:
	# 	df2 = pd.read_csv('../raw_data/2017-11-'+str(day)+'/checking.csv')
	# 	id_all_2 = []
	# 	for i in df2['id']:
	# 		id_all_2.append(i)
	#
	# 	id_all_2 = list(set(id_all_2))
	# 	id_all_2.sort()

	# 	if id_all_2!=id_1:
	# 		print(day)


	df_find_from, df_find_to = find_email()
	# df_find_from.to_csv('from_1067.csv', encoding='gbk')
	# df_find_to.to_csv('to_1067.csv', encoding='gbk')

	subject_from = df_find_from['subject']
	subject_from = list(set(subject_from))

	print(subject_from)

	subject_to = df_find_to['subject']
	subject_to = list(set(subject_to))

	print(subject_to)
	# leader_find_email()

	print('------end------')
	end_time = time.time()
	print('cost time: '+str(end_time - start_time) +' s')


