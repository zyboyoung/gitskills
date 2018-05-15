# 将统计得到的三个部门收发邮件的频率转化为json格式：[{"value": [], "name": ' '},{}]
import pandas as pd
import json


def to_json(file):
    df = pd.read_csv('email_frequency/hr_from_frequency.csv')
    json_list = []
    for index, row in df.iterrows():
        name = row['id']
        sub_dict = {}
        value = []
        value.append(int(row['from']))
        value.append(int(row['to']))
        value.append(int(row['total_email']))
        value.append(int(row['from_person']))
        value.append(int(row['to_person']))
        value.append(int(row['total_person']))
        sub_dict["value"] = value
        sub_dict["name"] = str(name)

        json_list.append(sub_dict)

    print(json_list)
    return json.dumps(json_list, indent=4)


if __name__=='__main__':
    id_class = ['hr', 'finance', 'dev']
    for id in id_class:
        file = 'email_frequency/'+id+'_from_frequency.csv'
        try:
            with open('email_frequency/'+id+'_email.json', 'w') as f:
                f.write(to_json(file))
            print(id+'写入完成')
        except:
            pass