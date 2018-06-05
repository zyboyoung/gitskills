import pandas as pd


def find_group(key_id):
	df = pd.read_csv('员工id对应收发件(dev).csv', encoding='gbk', engine='python')
	df = df.astype(str)
	df_group = df[df['from'].str.contains(key_id) | df['to'].str.contains(key_id)].drop_duplicates()
	id_group = []
	id_group.extend(df_group['from'])
	id_group.extend(df_group['to'])
	# print('------ group of '+key_id+' ------')
	# print(list(set(id_group)))
	groups.extend(id_group)


if __name__=='__main__':
	key_1007 = ['1125', '1087', '1115', '1092', '1172', '1199', '1230', '1224', '1192']
	# 1007除了给每个group的小leader发送过邮件外，也和1281员工有邮件往来
	key_1059 = ['1101', '1155', '1057', '1096', '1119', '1058', '1228', '1080', '1211', '1143']
	key_1068 = ['1060', '1191', '1154', '1207', '1209', '1100', '1098']
	groups = []

	for key_id in key_1068:
		find_group(key_id)

	for key_id in key_1007:
		find_group(key_id)

	for key_id in key_1059:
		find_group(key_id)

	groups = list(set(groups))
	print(groups)
	print(len(groups))

	print(len(groups))
	groups_new = []
	for i in groups:
		groups_new.append(int(i))

	df = pd.read_csv('员工id对应收发件(dev).csv', encoding='gbk', engine='python')
	from_list = list(df['from'].drop_duplicates())
	to_list = list(df['to'].drop_duplicates())
	to_list.extend(from_list)
	to_list = list(set(to_list))

	c = [i for i in to_list if i not in groups_new]
	print(c)
