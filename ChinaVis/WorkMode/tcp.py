# tcp的服务器端口/协议，上下行流量，以及流量差与流量和
import pandas as pd
import os
from time import time


# 找到所有出现过的协议
def find_proto(file):
    proto_list = []
    df = pd.read_csv(file)
    proto_list.extend(df['proto'])
    proto_list = list(set(proto_list))

    print(proto_list)


# 对于每个协议，找到对应的服务器端口
def find_dport(file):
    df = pd.read_csv(file)
    tcp_dict = {}
    for key in ['tds', 'smtp', 'sftp', 'mysql', 'http', 'ftp', 'postgresql', 'ssh', 'mongodb']:
        df_proto = df[(df['proto']==key)]
        dport = []
        dport.extend(df_proto['dport'])
        tcp_dict[key] = list(set(dport))

    print(tcp_dict)


# 统计每天的服务器协议/端口，上下行流量，计算流量和与流量差
def deal_tcp(file):
    df_log_info = pd.DataFrame(columns=['time', 'proto', 'dport', 'uplink_length', 'downlink_length', 'sumlink_length', 'difflink_length'])
    df = pd.read_csv(file)
    df = df.drop(['dtime', 'sip', 'dip', 'sport'], axis=1)
    time = list(df['stime'].drop_duplicates())[0].split()[0]

    i = 0
    for key in ['tds', 'smtp', 'sftp', 'mysql', 'ftp', 'postgresql', 'ssh', 'mongodb']:
        df_key = df[(df['proto']==key)]
        dport = list(df_key['dport'].drop_duplicates())[0]
        df_log_info.loc[i] = {'time': time, 'proto': key, 'dport': dport, 'uplink_length': df_key.iloc[:, 2].sum(), 'downlink_length': df_key.iloc[:, 3].sum(), 'sumlink_length': df_key.iloc[:, 2].sum()+df_key.iloc[:, 3].sum(), 'difflink_length': df_key.iloc[:, 2].sum()-df_key.iloc[:, 3].sum()}
        i += 1

    df_http = df[(df['proto']=='http')]
    df_log_info.loc[i] = {'time': time, 'proto': 'http', 'dport': 'NA', 'uplink_length': df_http.iloc[:, 2].sum(), 'downlink_length': df_http.iloc[:, 3].sum(), 'sumlink_length': df_http.iloc[:, 2].sum() + df_http.iloc[:, 3].sum(), 'difflink_length': df_http.iloc[:, 2].sum() - df_http.iloc[:, 3].sum()}

    print(df_log_info)
    return df_log_info


if __name__=='__main__':
    start_time = time()

    pd.set_option('display.width', 1000)

    root_path = '../raw_data/2017-11-'
    num = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
    other_num = list(range(10, 31))
    num.extend(other_num)

    df = pd.DataFrame(columns=['time', 'proto', 'dport', 'uplink_length', 'downlink_length', 'sumlink_length', 'difflink_length'])

    for i in num:
        file = root_path+str(i)+'/tcpLog.csv'
        df_log_info = deal_tcp(file)
        print('\n')
        df = pd.concat([df, df_log_info])

    print(df)
    try:
        df.to_csv('tcp统计.csv')
    except:
        pass

    end_time = time()
    print('cost time: ' + str(end_time - start_time) + ' s')