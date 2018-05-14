# 统计所有weblog.csv中的host，并对其进行三分类：工作、娱乐、其它

import pandas as pd
from time import time
import os
import collections

# 获取所有将要读取的csv文件所在的目录名
source_path = '../raw_data'
files_path = []
for root, dirs, files in os.walk(source_path):
	files_path.append(root)

files_path.remove(source_path)

# 统计所有host的频次
def count_web(path):
	host_csv = pd.DataFrame()
	for file_path in path:
		df = pd.read_csv(file_path+'\\weblog.csv')
		host_column = df['host']
		host_column = host_column.dropna(axis=0)
		top_5(host_column)
		host_csv = pd.concat([host_csv, host_column])

	host_list = list(host_csv[0])
	hosts = collections.Counter(host_list)
	host_csv = host_csv.drop_duplicates()
	print('------所有登录的host如下所示：------\n')
	print(hosts, '\n')
	return host_csv

# 统计每天登录前五名的网址
def top_5(host_csv):
	host_list = list(host_csv)
	hosts = collections.Counter(host_list)
	print(hosts.most_common(5))

# 对所有的host进行三分类
def classify(host_csv):
	pattern_work = 'hightech|csdn|ruanyifeng|programmer|infoq|teamtreehouse|xiaoxia|vpsee|mypm|programfan|theserverside|2ccc|uml\.org'
	hosts_string = host_csv.astype(str)
	work_host = hosts_string[hosts_string[0].str.contains(pattern_work)]
	print('------所有工作的host如下所示：------\n')
	print(work_host, '\n')

	pattern_entertainment = 'sohu|yahoo|taobao|sina|1ting|hupu|amazon|baihe|v\.6|tudou|zhibo|nba|tianya|9ku|qidian|17173|6\.cn|ifeng|acfun|zhulang|narutom|sports|jiayuan|readnovel|v\.qq|yinyuetai|3366|music'
	entertainment_host = hosts_string[hosts_string[0].str.contains(pattern_entertainment)]
	print('------所有娱乐的host如下所示：------\n')
	print(entertainment_host, '\n')

	others_host = host_csv.append(work_host).append(entertainment_host).drop_duplicates(keep=False)
	print('------所有其它的host如下所示：------\n')
	print(others_host, '\n')

if __name__=='__main__':
	host_csv = count_web(files_path)
	classify(host_csv)
