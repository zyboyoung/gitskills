# 找出主题含有EmergencyDataBase的邮件，统计其出现的时间、ip、收件人，并查询在相同时间内公司内部协议的上下行流量，重点关注三个数据库协议

import pandas as pd
from time import time
from matplotlib import pyplot as plt
import datetime

pd.set_option('display.width', 1000)
file_path = '../raw_data/2017-11-'
num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
other_num = list(range(10, 31))
num.extend(other_num)


# 发现时间集中为11-16日的20点到21点30，发送方为alert@hightech.com，接收方id为1284，1487（考虑查一下这两人是否担任职务），sip为10.63.120.70，dip为10.5.71.60
def emergency_email():
	emergency = pd.DataFrame()

	for i in num:
		file_root = file_path + str(i)
		df = pd.read_csv(file_root + '/new.csv', encoding='gbk')
		df = df.astype(str)
		df = df[df['subject'].str.contains('EmergencyDataBase')]
		emergency = pd.concat([df, emergency])

	print(emergency)


def database_log():

	for key in ('mongodb', 'mysql', 'postgresql'):
	# for key in ('tds',):
		uplink_key = []
		downlink_key = []
		uplink = []
		downlink = []

		for i in num:
			file_root = file_path + str(i)
			df = pd.read_csv(file_root + '/tcpLog.csv')
			time = '2017-11-'+str(i)

			df['uplink_length'] = df['uplink_length'] * 8 / 1024 / 1024
			df['downlink_length'] = df['downlink_length'] * 8 / 1024 / 1024

			df_key = df[(df['stime'] > (time+' 20:00:00')) & (df['stime'] < (time+' 21:30:00')) & (df['proto'] == key)].reset_index(drop=True)
			df_all = df[(df['stime'] > (time+' 20:00:00')) & (df['stime'] < (time+' 21:30:00'))].reset_index(drop=True)

			uplink_key.append(df_key.iloc[:, 7].sum())
			downlink_key.append(df_key.iloc[:, 8].sum())

			uplink.append(df_all.iloc[:, 7].sum())
			downlink.append(df_all.iloc[:, 8].sum())

		x = range(1, 31)

		try:
			proto_span = pd.DataFrame({'time': x, 'uplink': uplink_key, 'down': downlink_key})
			proto_span.to_csv('span_'+key+'.csv')
		except:
			pass

		plt.plot(x, uplink_key, color='green', label='uplink')
		plt.plot(x, downlink_key, color='blue', label='downlink')
		# plt.plot(x, uplink, color='red', label='uplink')
		# plt.plot(x, downlink, color='blue', label='downlink')

		plt.title(key)
		plt.legend(loc='lower right')
		plt.show()


def database_log_seven2nine30():
	for key in ('mongodb', 'mysql', 'postgresql', 'tds'):

	# for key in ('tds',):
		uplink_key = []
		downlink_key = []
		uplink = []
		downlink = []


		df = pd.read_csv('../raw_data/2017-11-16/tcpLog.csv')

		df['uplink_length'] = df['uplink_length'] * 8 / 1024 / 1024
		df['downlink_length'] = df['downlink_length'] * 8 / 1024 / 1024

		df_key = df[(df['stime'] > ('2017-11-16 19:00:00')) & (df['stime'] < ('2017-11-16 21:30:00')) & (df['proto'] == key)].reset_index(drop=True)
		df_all = df[(df['stime'] > ('2017-11-16 19:00:00')) & (df['stime'] < ('2017-11-16 21:30:00'))].reset_index(drop=True)

		print(df_key)
		df_key.to_csv('span_' + key + '(7-9.5).csv')


# 将一个字符串表示的时间+5分钟，返回字符串表示的时间
def next_5mins_time(time):
	str2datetime = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
	next_5mins_datetime = str2datetime + datetime.timedelta(minutes=+5)
	next_5mins_str = next_5mins_datetime.strftime('%Y-%m-%d %H:%M:%S')

	return next_5mins_str


# 按五分钟进行分段统计流量
def database_log_5min():
	df_mongodb = pd.read_csv('span_mongodb(7-9.5).csv')
	df_mysql = pd.read_csv('span_mysql(7-9.5).csv')
	df_postgresql = pd.read_csv('span_postgresql(7-9.5).csv')
	df_tds = pd.read_csv('span_tds(7-9.5).csv')

	time = '2017-11-16 19:00:00'
	df = pd.DataFrame(columns=['time', 'mongodb_uplink', 'mongodb_downlink', 'mysql_uplink', 'mysql_downlink', 'postgresql_uplink', 'postgresql_downlink', 'tds_uplink', 'tds_downlink'])
	i = 0

	while(time <= '2017-11-16 21:25:00'):
		next_time = next_5mins_time(time)
		df_mongodb_span = df_mongodb[(df_mongodb['stime'] > time) & (df_mongodb['stime'] < next_time)].reset_index(drop=True)
		mongodb_uplink = df_mongodb_span.iloc[:, 7].sum()
		mongodb_downlink = df_mongodb_span.iloc[:, 8].sum()

		df_mysql_span = df_mysql[(df_mysql['stime'] > time) & (df_mysql['stime'] < next_time)].reset_index(drop=True)
		mysql_uplink = df_mysql_span.iloc[:, 7].sum()
		mysql_downlink = df_mysql_span.iloc[:, 8].sum()

		df_postgresql_span = df_postgresql[(df_postgresql['stime'] > time) & (df_postgresql['stime'] < next_time)].reset_index(drop=True)
		postgresql_uplink = df_postgresql_span.iloc[:, 7].sum()
		postgresql_downlink = df_postgresql_span.iloc[:, 8].sum()

		df_tds_span = df_tds[(df_tds['stime'] > time) & (df_tds['stime'] < next_time)].reset_index(drop=True)
		tds_uplink = df_tds_span.iloc[:, 7].sum()
		tds_downlink = df_tds_span.iloc[:, 8].sum()

		df.loc[i] = {'time': time, 'mongodb_uplink': mongodb_uplink, 'mongodb_downlink': mongodb_downlink, 'mysql_uplink': mysql_uplink, 'mysql_downlink': mysql_downlink, 'postgresql_uplink': postgresql_uplink, 'postgresql_downlink': postgresql_downlink, 'tds_uplink': tds_uplink, 'tds_downlink': tds_downlink}

		i += 1
		time = next_time

	print(df)
	df.to_csv('database_log_5min.csv')


if __name__ == '__main__':
	start_time = time()

	# emergency_email()

	# database_log()

	# 统计11.16晚上7点到9点半三种数据库协议上下行流量情况
	# database_log_seven2nine30()

	# 按五分钟为时间段对7-9.5.csv划分三种数据库上下行流量
	database_log_5min()

	end_time = time()
	print('------运行时间为' + str(end_time - start_time) + ' s------')
