# 统计每天各协议上行流量和下行流量，并作出折线趋势图

import pandas as pd
from matplotlib import pyplot as plt

pd.set_option('display.width', 1000)


def cal_tcp(proto):
    df = pd.read_csv('../WorkMode/tcp统计.csv', encoding='gbk', engine='python')
    df_proto = df[(df['proto'] == proto)].drop(['sumlink_length', 'difflink_length', 'dport', 'Unnamed: 0', 'proto', 'time'], axis=1).set_index([list(range(1, 31))])
    df_proto['uplink_length'] = df_proto['uplink_length'] * 8 / 1024 /1024
    df_proto['downlink_length'] = df_proto['downlink_length'] * 8 / 1024 /1024
    print(df_proto)

    # 将columns的数据趋势分开显示
    df_proto.plot(subplots=True)

    plt.xticks(range(1, 31), df_proto.index)
    plt.legend(loc='lower right')
    plt.title(proto)
    plt.show()

    return df_proto

if __name__=='__main__':
    for key in ('tds', 'smtp', 'ssh', 'sftp', 'mysql', 'ftp', 'postgresql', 'mongodb', 'http'):
        df_proto = cal_tcp(key)

        df_proto.to_csv('proto_'+key+'.csv')