# Alarm、安全邮件崩溃、互联网资产监控报警、recover相关邮件提出来分析，每天什么时间发起

import pandas as pd
from matplotlib import pyplot as plt
from pylab import mpl

pd.set_option('display.width', 1000)
mpl.rcParams['font.sans-serif'] = ['FangSong']

file_path = '../raw_data/2017-11-'
num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
other_num = list(range(10, 31))
num.extend(other_num)


def raise_time(file, key):
	df = pd.read_csv(file, encoding='gbk')
	df_reset = df.astype(str)
	df_reset = df_reset[df_reset['subject'].str.contains(key)]

	print(df_reset)
	return len(df_reset)


if __name__=='__main__':
	emails_count = []
	time = []
	for key in ['ALARM', '安全邮件崩溃', '互联网资产监控报警', 'RECOVER']:

		email_count = []
		for i in num:
			file_root = file_path + str(i)
			file = file_root + '/new.csv'

			print('------主题为'+key+'的邮件如下------')
			if len(time)!=30:
				time.append(str(i))
			email_count.append(raise_time(file, key))

		emails_count.append(email_count)

	print(time)
	print(emails_count[0])
	plt.plot(time, emails_count[0], color = 'green', label='ALARM')
	plt.plot(time, emails_count[1], color = 'blue', label='安全邮件崩溃')
	plt.plot(time, emails_count[2], color = 'red', label='互联网资产监控报警')
	plt.plot(time, emails_count[3], color = 'purple', label='RECOVER')

	plt.legend(loc='lower right', fontsize=10)
	plt.xticks(range(30), time, fontsize=8)
	plt.show()

