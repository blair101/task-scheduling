#!/usr/bin/python
# -*- coding:utf-8 -*-
import pymysql
import re
import sys
import importlib
importlib.reload(sys)
#sys.setdefaultencoding('utf-8')

def get_table_info(table, database, schema='', ispartition=True):
    '''
    table =  为表名，mysql,hive表名一致
    schema = 为hive中的库名
    ispartition : 是否分区默认为分区
    '''
    cols = []
    create_head = '''create external table if not exists {0}.{1} (\n'''.format(schema, table)
    if ispartition:
        create_tail = r'''
partitioned by (dt string)
row format delimited fields terminated by '\001' collection items terminated by ',' map keys terminated by ':' lines terminated by '\n'
stored as textfile
location '/ext_dc/{0}/{1}';'''.format(schema, table)
    else:
        create_tail = r'''
row format delimited fields terminated by '\u0001' collection items terminated by ',' map keys terminated by ':' lines terminated by '\n'
stored as textfile
location '/ext_dc/{0}/{1}';'''.format(schema, table)
    connection = pymysql.connect(host='$host',
                                 user='$user',
                                 password='$password',
                                 db=database,
                                 port=3306,
                                 charset='utf8'
                                 )
    try:
        with connection.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            sql = 'SHOW FULL FIELDS FROM  {0}'.format(table)
            cout = cursor.execute(sql)  
            try:
                for row in cursor:  # cursor.fetchall()
                    # print(row)
                    cols.append(row['Field'])
                    if 'bigint' in row['Type']:
                        row['Type'] = "bigint"
                    elif 'int' in row['Type'] or 'tinyint' in row['Type'] or 'smallint' in row['Type'] or 'mediumint' in \
                            row['Type'] or 'integer' in row['Type']:
                        row['Type'] = "int"
                    elif 'double' in row['Type'] or 'float' in row['Type'] :
                        row['Type'] = "double"
                    elif 'varchar' in row['Type'] :
                        row['Type'] = "varchar(64)"
                    elif 'decimal' in row['Type'] :
                        row['Type'] = "decimal"
                    elif 'char' in row['Type'] :
                        row['Type'] = "char(32)"
                    elif 'datetime' in row['Type'] :
                        row['Type'] = "timestamp"
                    else:
                        row['Type'] = "string"
                    create_head += '    ' + row['Field'] + ' ' + row['Type'] + ' comment \'' + row['Comment'] + '\' ,\n'
            except:
                print('程序异常!')
    finally:
        connection.close()
    create_str = create_head[:-2] + '\n' + ')' + create_tail
    return cols, create_str  

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("start...")
        print(len(sys.argv))
        exit(1)
        print("end!!!")

    db_table=sys.argv[1]

    database=db_table.split('.')[0]
    table=db_table.split('.')[1]
    schema=sys.argv[2]
   
    if len(sys.argv) == 4 and sys.argv[3] == 'no':
        ispartition=False
    else:
        ispartition=True

    #tables=['asset_credit_info','dim_customer','ods_loan_invoice','rpt_131_overdue_detail','snp_loan_invoice']
    #for table in tables: 
    cols, create_str = get_table_info(table,database,schema,ispartition)
        # print(cols)
    print(create_str)
