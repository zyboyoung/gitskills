import pandas as pd 
import os

# 得到30个文件的名字
root_path = '../raw_data'
file_path = []
for root, dirs, files in os.walk(root_path):
    file_path.append(root)

def one_to_some(file_name):
    """
    之前已经将每天的邮件按照部门进行了划分，在此之上，筛选出每天相应部门中一对多的发送邮件的情况
    """
    df = pd.read_csv(file_name, encoding='gbk')
    df_new = pd.DataFrame()

    for index, row in df.iterrows():
        to_list = row['to'].split(';')
        if len(to_list) > 1:
            df_new = pd.concat([df_new, df[df.index==index]])
    
    return df_new


if __name__=='__main__':
    id_class = 'dev'
    df = pd.DataFrame()

    for file in file_path:
        file_name = file+'/email_'+id_class+'.csv'
        df = pd.concat([df, one_to_some(file_name)])
    
    df.to_csv(id_class+'_one_to_some.csv')



