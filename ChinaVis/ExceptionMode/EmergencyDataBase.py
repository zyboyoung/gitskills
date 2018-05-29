# 找出主题含有EmergencyDataBase的邮件，统计其出现的时间、ip、收件人，并查询在相同时间内公司内部协议的上下行流量，重点关注三个数据库协议

import pandas as pd
from time import time
from matplotlib import pyplot as plt

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
	# for key in ('mongodb', 'mysql', 'postgresql'):
	for key in ('tds',):
		uplink_key = []
		downlink_key = []
		uplink = []
		downlink = []
		for i in num:
			file_root = file_path + str(i)
			df = pd.read_csv(file_root + '/tcpLog.csv')
			time = '2017-11-'+str(i)

			df_key = df[(df['stime'] > (time+' 20:00:00')) & (df['stime'] < (time+' 21:30:00')) & (df['proto'] == key)].reset_index(drop=True)
			df_all = df[(df['stime'] > (time+' 20:00:00')) & (df['stime'] < (time+' 21:30:00'))].reset_index(drop=True)

			uplink_key.append(df_key.iloc[:, 7].sum())
			downlink_key.append(df_key.iloc[:, 8].sum())

			uplink.append(df_all.iloc[:, 7].sum())
			downlink.append(df_all.iloc[:, 8].sum())

		x = range(1, 31)
		plt.plot(x, uplink_key, color='green', label='uplink')
		plt.plot(x, downlink_key, color='blue', label='downlink')
		# plt.plot(x, uplink, color='red', label='uplink')
		# plt.plot(x, downlink, color='blue', label='downlink')

		plt.title(key)
		plt.legend(loc='lower right')
		plt.show()


if __name__ == '__main__':
	start_time = time()

	# emergency_email()

	database_log()

	end_time = time()
	print('------运行时间为' + str(end_time - start_time) + ' s------')
