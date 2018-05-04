import csv

raw_file_path = '../raw_data/2017-11-'
num = list(range(10, 31))
other_num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
num.extend(other_num)

if __name__=='__main__':
	for i in num:
		new_file = open(raw_file_path + str(i) + '/new.csv', 'w', newline='')
		writer = csv.writer(new_file)
		with open(raw_file_path + str(i) + '/email.csv', newline='', encoding='gb2312', errors='replace') as file:
			reader = csv.reader(file)
			for row in reader:
				try:
					writer.writerow(row)
				except UnicodeEncodeError:
					continue