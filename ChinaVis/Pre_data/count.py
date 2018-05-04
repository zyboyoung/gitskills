import pandas as pd

days = list(range(10, 31))
other_days = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
days.extend(other_days)

count = 0
for day in days:
	df = pd.read_csv('../raw_data/2017-11-'+str(day)+'/new.csv', encoding='gbk')
	count += len(df)

print(count)