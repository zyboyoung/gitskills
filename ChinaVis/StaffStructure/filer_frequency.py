# 将接收和发送频次的两个csv合并为一个，并删除原有csv
# 按日期筛选一天接收或者发送过多数量邮件的id，返回一个csv
import pandas as pd
import os
from time import time

def combine_csv():
	for id_class in ('hr', 'finance', 'dev'):
		df_from = pd.read_csv(id_class+'_from_frequency.csv')
		df_to = pd.read_csv(id_class+'_to_frequency.csv')

		df = pd.merge(df_from, df_to, on='id', suffixes=('_from', '_to'))
		df.to_csv(id_class+'_frequency.csv')

		os.remove(id_class+'_from_frequency.csv')
		os.remove(id_class+'_to_frequency.csv')


def filter(day, id_class):
	df_id = pd.read_csv(id_class+'_frequency.csv')
	file_path = '../raw_data/2017-11-'
	df_email = pd.read_csv(file_path+str(day)+'/new.csv', encoding='gbk')

	df = pd.DataFrame(columns=['id', 'date', 'from', 'to'])
	for id in df_id['id']:
		df_from = df_email[df_email['from'].str.contains(str(id) + '@hightech\.com')]
		df_to = df_email[df_email['to'].str.contains(str(id) + '@hightech\.com')]

		if id_class=='dev':
			if len(df_from)>5 or len(df_to)>10:
				dict = {'id': id, 'date': '2017-11-'+str(day), 'from': len(df_from), 'to': len(df_to)}
				# 将字典转为DataFrame
				df_id = pd.DataFrame.from_dict(dict, orient='index').T
				df = pd.concat([df, df_id])

		elif id_class=='hr':
			if len(df_from)>10 or len(df_to)>40:
				dict = {'id': id, 'date': '2017-11-' + str(day), 'from': len(df_from), 'to': len(df_to)}
				df_id = pd.DataFrame.from_dict(dict, orient='index').T
				df = pd.concat([df, df_id])

		elif id_class=='finance':
			if len(df_from)>5 or len(df_to)>40:
				dict = {'id': id, 'date': '2017-11-' + str(day), 'from': len(df_from), 'to': len(df_to)}
				df_id = pd.DataFrame.from_dict(dict, orient='index').T
				df = pd.concat([df, df_id])

	return df


if __name__=='__main__':
	start_time = time()

	# combine_csv()

	days = list(range(10, 31))
	other_days = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
	days.extend(other_days)
	df = pd.DataFrame(columns=['id', 'date', 'from', 'to'])

	id_class = 'finance'
	for day in days:
		df_day = filter(day, id_class)
		df = pd.concat([df, df_day])

	df.to_csv('exceptional_frequency/' + id_class + '_exception.csv')
	print('------end------')
	end_time = time()
	print('cost time: '+str(end_time-start_time)+' s')