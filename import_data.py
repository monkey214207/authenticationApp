# encoding=utf-8
import csv
import pymysql
import codecs
import time
import os

'''
python读取csv文件到mysql上
'''


def read_table_head(filename, head_file='./data/heads.csv'):
    heads_dict = {}
    with open(head_file, "r") as heads:
        reader = csv.reader(heads)
        for line in reader:
            line = list(filter(lambda x: x != '', line))
            heads_dict[line[1]] = line[2:]
    return heads_dict[filename]


class PyMysql:
    def __init__(self, table, db):
        self.db = db
        self.table = table

    # 连接数据库
    def conn_mysql(self):
        conn = pymysql.connect(
            host='localhost',  # 你的主机IP
            port=3309,  # 主机端口，不能加双引号
            user='authApp',  # MySQL用户
            password='Dev#2020',  # MySQL密码
            charset='utf8'  # 使用的编码格式，不能使用  utf-8 ，不能加多一个横杠
        )
        return conn

    # 创建数据库
    def create_db(self, cur):
        db = cur.cursor()  # 创建光标
        db.execute("create database if not exists {} character set utf8;".format(self.db))  # 创建数据库
        db.execute("use {};".format(self.db))  # 切换至改数据库
        cur.commit()  # 一定要进行事务更新
        print('创建数据库成功')
        return cur

    # 创建表
    def create_table_head(self, db, head):
        sql = 'create table if not exists {}('.format(self.table)  # 创建表
        for i in range(0, len(head)):
            sql += '{} varchar(100)'.format(head[i])
            if i != len(head) - 1:
                sql += ','
            sql += '\n'
        sql += ');'
        cur = db.cursor()  # 创建光标
        cur.execute(sql)  # 执行命令
        db.commit()  # 一定要进行事务更新
        time.sleep(0.1)
        print('创建表完成')

    # 插入数据
    def insert_table_info(self, db, info):
        sql = 'insert into {} values ('.format(self.table)
        for i in range(0, len(info)):
            sql += '"{}" '.format(info[i])
            if i != len(info) - 1:
                sql += ','
        sql += ');'
        try:
            cur = db.cursor()
            cur.execute(sql)
            db.commit()  # 一定要进行事务更新
            # time.sleep(0.1)
            # print('插入数据成功')
        except Exception as e:
            print('插入数据失败,失败原因', e)

    # 创建表所需要的字段
    def table_head(self, filename):
        with codecs.open(filename=filename, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            head = next(reader)
            return head

    # 表需要的数据
    def table_info(self, db, filename):
        with codecs.open(filename=filename, mode='r', encoding='utf-8') as f:
            data = csv.reader(f)
            for index, row in enumerate(data):
                self.insert_table_info(db, row)


if __name__ == '__main__':
    # need to change to your local data storage path
    path = '/Users/yumei.hou/Documents/data-storage/public_dataset/100669/100669_session_1'
    for csvFile in os.listdir(path):
        if os.path.splitext(csvFile)[1] == '.csv':
            table_name = csvFile[:-4]
            pysql = PyMysql(table_name, 'auth')
            cur = pysql.conn_mysql()  # 连接mysql数据库
            db = pysql.create_db(cur)  # 创建数据库
            filename = os.path.join(path, table_name + '.csv')  # 插入数据的文件
            print(filename)
            head = read_table_head(table_name + '.csv')
            pysql.create_table_head(db, head)  # 创建表
            pysql.table_info(db, filename)  # 插入数据
