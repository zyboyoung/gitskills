# 根据login的协议/端口分类，保留state/success的比例

import pandas as pd
import os
from time import time


# 分类协议/端口，统计频次，success的比例
def deal_log(file):
    df_log_info = pd.DataFrame(columns=['time', 'proto', 'dport', 'frequency', 'success'])
    df = pd.read_csv(file)
    df_del_columns = df.drop(['dip', 'sip', 'sport', 'user'], axis=1)
    print('总数据量为' + str(len(df_del_columns)))
    time = list(df['time'].drop_duplicates())[0].split()[0]

    i = 0
    for key in ['sftp', 'mongodb', 'ssh', 'postgresql', 'ftp', 'tds', 'mysql']:
        df_key = df_del_columns[(df_del_columns['proto'] == key)]
        dport = list(set(list(df_key['dport'])))[0]
        frequency = len(df_key)
        success = len(df_key[(df_key['state'] == 'success')]) / frequency

        df_log_info.loc[i] = {'time': time, 'proto': key, 'dport': dport, 'frequency': frequency, 'success': success}
        i += 1
    print(df_log_info)
    print('\n')
    return df_log_info


if __name__=='__main__':
    start_time = time()

    pd.set_option('display.width', 1000)

    root_path = '../raw_data/2017-11-'
    num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
    other_num = list(range(10, 31))
    num.extend(other_num)

    df_log_info = pd.DataFrame(columns=['time', 'proto', 'dport', 'frequency', 'success'])

    for i in num:
        file = root_path+str(i)+'/login.csv'
        print('2017-11-'+str(i))
        df_log_info = pd.concat([df_log_info, deal_log(file)]).reset_index(drop=True)

    print(df_log_info)
    try:
        df_log_info.to_csv('login统计.csv')
    except:
        pass

    end_time = time()
    print('\ncost time: ' + str(end_time - start_time) + ' s')