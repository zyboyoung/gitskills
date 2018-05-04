import pandas as pd 
import os
import re

# 得到30个文件的名字
root_path = '../raw_data'
file_path = []
for root, dirs, files in os.walk(root_path):
    file_path.append(root)

file_path.remove(root_path)

def one_to_some(file_name):
    """
    之前已经将每天的邮件按照部门进行了划分，在此之上，筛选出每天相应部门中一对多的发送邮件的情况
    """
    df = pd.read_csv(file_name, encoding='gbk')
    df_new = pd.DataFrame()

    for index, row in df.iterrows():
        to_list = row['to'].split(';')
        if len(to_list) > 1:
            df_new = pd.concat([df_new, df[df.index==index]], ignore_index=True)

    df_new = df_new.reset_index(drop=True)
    return df_new


def print_dict(df):
    df_new = pd.DataFrame(columns=['from', 'to'])
    pattern = re.compile(r'(\d{4})@hightech\.com')
    i = 0

    for _, row in df.iterrows():
        id_from = re.search(pattern, row['from']).group(1)
        id_to = re.findall(pattern, row['to'])
        df_new.loc[i] = {'from': id_from, 'to': tuple(id_to)}
        i += 1

    df_new = df_new.drop_duplicates()

    return df_new

if __name__=='__main__':
    id_class = 'hr'
    df = pd.DataFrame()

    for file in file_path:
        file_name = file+'\email_'+id_class+'.csv'
        df = pd.concat([df, one_to_some(file_name)])

    try:
        df.to_csv('one_to_some\\'+id_class+'_one_to_some.csv', encoding='gbk')
        print('已生成'+id_class+'_one_to_some文件')
    except:
        print(df)
        print('该文件已经存在')

    df_new = print_dict(df)
    try:
        df_new.to_csv('one_to_some\id_'+id_class+'_one_to_some.csv')
        print('已生成id_'+id_class+'_one_to_some文件')
    except:
        print(df_new)
        print('该文件已经存在')



