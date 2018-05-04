# 将id对应的频次画图
import pandas as pd
from matplotlib import pyplot as plt

def plot_frequency(id_class):
	df_from = pd.read_csv(id_class+'_from_frequency.csv')
	df_to = pd.read_csv(id_class+'_to_frequency.csv')
	#
	# print(df_from['id'])
	# print(df_to['id'])
	# exit(1)
	if df_from['id'].all() == df_to['id'].all():
		X = df_from['id']
		Y_from = df_from['frequency']
		Y_to = df_to['frequency']

		plt.xlim(1000, 1500)
		plt.ylim(0, 400)

		for i in range(len(Y_to)):
			if Y_to[i] > 400:
				Y_to[i] = 400

		plt.yticks(fontsize=15)
		plt.xticks(fontsize=15)

		plt.title('from_to_frequency of '+id_class, fontsize=25)

		plt.xlabel('id', fontsize=18)
		plt.ylabel('frequency', fontsize=18)
		plt.plot(X, Y_from, color='green', linestyle='-', label='From_frequency')
		plt.plot(X, Y_to, color='red', linestyle='-', label='To_frequency')

		plt.legend(loc='higher left', fontsize=10)

		plt.show()


if __name__=='__main__':
	id_class = 'dev'
	plot_frequency(id_class)

