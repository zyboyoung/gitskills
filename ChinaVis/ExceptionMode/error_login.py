import pandas as pd


# 对于给出的协议和文件，找到其中连接失败的记录
def find_error(proto, file):
    df = pd.read_csv(file)
    df_proto = df[(df['proto']==proto) & (df['state'] == 'error')]
    print(df_proto)

    return list(df_proto['dip'])[0], list(df_proto['sip'])[0], list(df_proto['user'])[0],


def find_ip(proto, dip, sip, file):
    df = pd.read_csv(file)
    if not df[(df['dip'] == dip) & (df['sip'] == sip)].empty:
        print(df[(df['dip'] == dip) & (df['sip'] == sip)])
    else:
        print('同样的sip与dip在当天没有其它协议的连接')
        print(df[(df['sip'] == sip)  & (df['proto']!=proto)])

    print(df[(df['proto'] == proto)])
    print(df[(df['dip'] == dip)])
    print(df[(df['sip'] == sip)])



if __name__=='__main__':
    pd.set_option('display.width', 1000)

    file = '../raw_data/2017-11-11/login.csv'
    proto = 'mysql'
    dip, sip, user = find_error(proto, file)

    find_ip(proto, '10.50.50.26', '10.64.105.59', file)
