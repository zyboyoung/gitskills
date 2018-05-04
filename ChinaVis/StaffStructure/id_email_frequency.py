# 用于统计每个id发送和接收邮件的频次
import pandas as pd
from time import time
import csv

# 通过id分别返回所有邮件中该id发送、接收的邮件信息，格式为DataFrame
def frequency(id):
	file_path = '../raw_data/2017-11-'
	num = list(range(10, 31))
	other_num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
	num.extend(other_num)

	df_from = pd.DataFrame()
	df_to = pd.DataFrame()

	for i in num:
		df = pd.read_csv(file_path + str(i) + '/new.csv', encoding='gbk')
		df_from_i = df[df['from'].str.contains(str(id)+'@hightech\.com')]
		df_from = pd.concat([df_from_i, df_from])

		df_to_i = df[df['to'].str.contains(str(id)+'@hightech\.com')]
		df_to = pd.concat([df_to_i, df_to])

	return df_from, df_to


if __name__=='__main__':
	start_time = time()

	id_class = 'hr'
	id_list = pd.read_csv('../Pre_data/id_' + id_class + '.csv')
	from_frequency = {}
	to_frequency = {}

	for id in id_list[id_class]:

		df_from_id, df_to_id = frequency(id)
		# 统计每个id发送、接收邮件的频次，并记录在字典中
		from_frequency[str(id)] = len(df_from_id)
		to_frequency[str(id)] = len(df_to_id)

	# 将字典保存为csv
	with open(id_class+'_from_frequency.csv', 'w', newline='') as f:
		fieldnames = ['id', 'frequency']
		writer = csv.writer(f)
		writer.writerow(fieldnames)
		for key, value in from_frequency.items():
			writer.writerow([key, value])

	with open(id_class+'_to_frequency.csv', 'w', newline='') as f:
		fieldnames = ['id', 'frequency']
		writer = csv.writer(f)
		writer.writerow(fieldnames)
		for key, value in to_frequency.items():
			writer.writerow([key, value])

	print('发送邮件的频次：', from_frequency, sep='\n')
	print('接收邮件的频次：', to_frequency, sep='\n')

	print('------end------')
	end_time = time()
	print('cost time: '+str(end_time-start_time)+' s')