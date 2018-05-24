# 分析3、10、17、24号的http上行流量，1.判断当天上行流量排名第一的条目，与第二名做对比，同时比较该人平时上行流量的值；2.统计当天记录的总条数，与往日总条数做对比

import pandas as pd
import os
from matplotlib import pyplot as plt

pd.set_option('display.width', 1000)


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


# 统计某sip在30天内的上下行流量，作折线图
def records_sip(sip):
	file_path = '../raw_data/2017-11-'
	num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
	other_num = list(range(10, 31))
	num.extend(other_num)

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
	print(df_new)

	df_new.plot(subplots=True)

	plt.xticks(range(30), time, fontsize=8)
	plt.legend(loc='lower right')
	plt.title(sip)
	plt.show()


if __name__=='__main__':

	# days = ['03', '10', '17', '24']
	# for day in days:
	# 	file = '../raw_data/2017-11-'+ day +'/tcpLog.csv'
	# 	# analysis(file, day)
	# 	grouped_sid(file, day)

	print('------10.50.50.26------')
	_ = records_sip('10.50.50.26')
	print('\n')
	print('------10.50.50.28------')
	_ = records_sip('10.50.50.28')
	print('\n')
	print('------10.50.50.29------')
	_ = records_sip('10.50.50.29')