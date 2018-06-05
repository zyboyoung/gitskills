import pandas as pd
import os
from matplotlib import pyplot as plt
from time import time

pd.set_option('display.width', 1000)
file_path = '../raw_data/2017-11-'
num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
other_num = list(range(10, 31))
num.extend(other_num)
odd_sips = ['217.12.13.41', '10.64.105.125', '220.181.90.34', '106.3.154.30', '204.79.197.203', '131.253.61.84', '113.108.216.17', '13.107.42.11', '203.78.142.12', '10.64.105.89', '114.80.130.60', '183.61.185.93', '58.251.61.186', '10.64.105.7', '123.58.177.21', '123.58.177.104', '123.58.180.7', '195.93.85.131', '106.3.154.69', '123.58.177.20', '211.150.82.8', '123.125.50.182', '101.236.29.216', '10.64.105.171', '172.217.160.101', '93.191.168.52', '10.64.106.4']

def analysis(file, day):
	df = pd.read_csv(file)
	df_reset = df[(df['proto'] == 'http') & (df['uplink_length'] > 0)].sort_values(by='uplink_length', ascending=False).reset_index(drop=True)

	df_top_10 = df_reset.head(10)
	print('------'+ day +'日上行流量最多的前十名------')
	print(df_top_10)

	tcp_diff = int(df_top_10.loc[0, ['uplink_length']] - df_top_10.loc[1, ['uplink_length']])
	if tcp_diff > int(df_top_10.loc[1, ['uplink_length']]) / 10:
		print('------'+ day +'日第一名的上行流量明显偏多------')
	else:
		print('------'+ day +'日没有明显偏多的上行流量，推测是记录条数增多------')
		length = sorted(records(), reverse=True)
		rank = length.index(len(df_reset))
		print('------上行流量记录如下，其中'+ day +'日上行流量排名为'+str(rank)+'------')
		print(length)


# 统计出每天上行流量的记录
def records():
	root_path = '../raw_data'
	paths = []
	for root, dirs, files in os.walk(root_path):
		paths.append(root)
	paths.remove(root_path)

	length = []

	for path in paths:
		df = pd.read_csv(path+'/tcpLog.csv')
		df_reset = df[(df['proto'] == 'http') & (df['uplink_length'] > 0)]
		length.append(len(df_reset))

	return length


# 按sip统计当天的上行流量，并给出排序
def grouped_sid(file, day):
	df = pd.read_csv(file)
	df_grouped = df['uplink_length'].groupby(df['sip']).sum()
	df_top_10 = df_grouped.sort_values(ascending=False).head(10)
	print('------' + day + '日按sip排名的上行流量------')
	print(df_top_10)
	print('\n')


# 找到所有使用过http协议的sip
def find_all_sip():
	all_sip = []
	df_http_sip = pd.DataFrame()

	for i in num:
		file_root = file_path + str(i)
		df = pd.read_csv(file_root + '/tcpLog.csv')
		df_http = df[(df['proto'] == 'http')]

		df_http_sip = pd.concat([df_http_sip, df_http])

	all_sip.extend(df_http_sip['sip'])
	all_sip = list(set(all_sip))

	print(all_sip)
	print(len(all_sip))

	return all_sip


# 统计某sip在30天内的上下行流量，作折线图
def records_sip(sip):
	time = []
	uplink_sum = []
	downlink_sum = []

	for i in num:
		file_root = file_path + str(i)
		df = pd.read_csv(file_root + '/tcpLog.csv')
		sip_uplink_length = df[(df['sip'] == sip)].iloc[:, 7].sum()
		sip_downlink_length = df[(df['sip'] == sip)].iloc[:, 8].sum()

		time.append(str(i))
		uplink_sum.append(sip_uplink_length)
		downlink_sum.append(sip_downlink_length)

	df_new = pd.DataFrame({'time': time, 'uplink': uplink_sum, 'downlink': downlink_sum})

	df_new_up_0 = df_new[(df_new['uplink'] == 0)]
	df_new_down_0 = df_new[(df_new['downlink'] == 0)]

	if len(df_new_down_0) > 15 or len(df_new_up_0) > 15:
		df_new.plot(subplots=True)
		plt.xticks(range(30), time, fontsize=8)
		plt.legend(loc='lower right')
		plt.title(sip)
		plt.show()
		sip_inactive.append(sip)

	del(df_new)


# 统计所有非活跃sip的协议流量
def odd_sip_all_proto(sip):
	for key in ['tds', 'smtp', 'sftp', 'mysql', 'ftp', 'postgresql', 'ssh', 'mongodb', 'http']:
		time = []
		uplink_sum = []
		downlink_sum = []

		for i in num:
			file_root = file_path + str(i)
			df = pd.read_csv(file_root + '/tcpLog.csv')
			sip_uplink_length = df[(df['sip'] == sip) & (df['proto'] == key)].iloc[:, 7].sum()
			sip_downlink_length = df[(df['sip'] == sip) & (df['proto'] == key)].iloc[:, 8].sum()

			time.append(str(i))
			uplink_sum.append(sip_uplink_length)
			downlink_sum.append(sip_downlink_length)

		df_new = pd.DataFrame({'time': time, 'uplink': uplink_sum, 'downlink': downlink_sum})

		if int(df_new.loc[:, ['uplink']].sum()) == 0 and int(df_new.loc[:, ['downlink']].sum()) == 0:
			print(sip+'没有进行过'+key+'协议传输流量')
		else:
			df_new.plot(subplots=True)
			plt.xticks(range(30), time, fontsize=8)
			plt.legend(loc='lower right')
			# plt.title(sip+'+'+key)
			plt.show()


# 找出不活跃ip中只访问http协议的ip
def odd_sip_only_http(sip):
	for key in ['tds', 'smtp', 'sftp', 'mysql', 'ftp', 'postgresql', 'ssh', 'mongodb']:
		for i in num:
			file_root = file_path + str(i)
			df = pd.read_csv(file_root + '/tcpLog.csv')
			sip_uplink_length = df[(df['sip'] == sip) & (df['proto'] == key)].iloc[:, 7].sum()
			sip_downlink_length = df[(df['sip'] == sip) & (df['proto'] == key)].iloc[:, 8].sum()
			if sip_uplink_length!=0 or sip_downlink_length!=0:
				return None
			else:
				continue
	return sip


# 对于不活跃sip，统计它们访问的网页
def odd_sip_web(sip):
	webs = []
	for i in num:
		file_root = file_path + str(i)
		df = pd.read_csv(file_root + '/weblog.csv')
		df_odd_sip = df[(df['sip'] == sip)]
		webs.extend(df['host'])

	print('------' + sip + '--------')
	print(set(webs))
	print('\n')


# 分析mongodb在3号、29号，以及mysql在9号，晚上8点到9点30的下行流量，找出流量最多的sip
def db_analysis(time, proto):
	df = pd.read_csv('../raw_data/'+time+'/tcpLog.csv')
	df_reset = df[(df['proto'] == proto) & (df['uplink_length'] > 0) & (df['stime'] > (time+' 20:00:00')) & (df['stime'] < (time+' 21:30:00')) ].sort_values(by='downlink_length', ascending=False).reset_index(drop=True)

	df_top_10 = df_reset.head(10)
	print('------'+ time +'日' + proto + '下行流量最多的前十名------')
	print(df_top_10)

	db_diff = int(df_top_10.loc[0, ['downlink_length']] - df_top_10.loc[1, ['downlink_length']])
	if db_diff > int(df_top_10.loc[1, ['downlink_length']]) / 10:
		print('------' + time + '日' + proto + '第一名的下行流量明显偏多------\n')
	else:
		print('------' + time + '日' + proto + '第一名的下行流量没有明显偏多------\n')
	# else:
	# 	print('------'+ time +'日没有明显偏多的下行流量，推测是记录条数增多------')
	# 	length = sorted(records(), reverse=True)
	# 	rank = length.index(len(df_reset))
	# 	print('------下行流量记录如下，其中'+ time +'日上行流量排名为'+str(rank)+'------')
	# 	print(length)


if __name__=='__main__':
	start_time = time()

	# 分析3、10、17、24号的http上行流量，1.判断当天上行流量排名第一的条目，与第二名做对比，同时比较该人平时上行流量的值；2.统计当天记录的总条数，与往日总条数做对比
	# days = ['03', '10', '17', '24']
	# for day in days:
	# 	file = '../raw_data/2017-11-'+ day +'/tcpLog.csv'
	# 	# analysis(file, day)
	# 	grouped_sid(file, day)


	# 分析上行流量最多的四个sip三十天的记录
	# print('------10.50.50.26------')
	# records_sip('10.50.50.26')
	# print('\n')
	# print('------10.50.50.28------')
	# records_sip('10.50.50.28')
	# print('\n')
	# print('------10.50.50.29------')
	# records_sip('10.50.50.29')


	# 找出所有使用过http协议的sip，并发现其中有明显活跃期与非活跃期（超过十天上行或下行流量为0）的sip
	# all_sip = find_all_sip()
	# i = 1
	# sip_inactive = []
	# for sip in all_sip:
	# 	print('------'+str(i)+'------')
	# 	records_sip(sip)
	# 	i += 1
	# print(sip_inactive)


	# 对于每个访问http不活跃的sip，统计其访问其它协议的流量
	for sip in odd_sips:
		odd_sip_all_proto(sip)


	# 对于每个不活跃的sip，进一步在其中找出不访问其它协议的sip
	# sips_only_http = []
	# for sip in odd_sips:
	# 	sips_only_http.append(odd_sip_only_http(sip))
	# print(sips_only_http)


	# 分析mongodb在3号、29号，以及mysql在9号，晚上8点到9点30的下行流量，找出流量最多的sip
	# db_analysis('2017-11-03', 'mongodb')
	# db_analysis('2017-11-29', 'mongodb')
	# db_analysis('2017-11-09', 'mysql')


	# 统计非活跃sip所访问的web
	# for sip in odd_sips:
	# 	odd_sip_web(sip)


	end_time = time()
	print('------运行时间为'+str(end_time - start_time)+' s------')