import mysql.connector


def dict_to_mysql_insert(table_name, data_dict):
    # 1. 确定列名（即字典的键）
    columns = ', '.join(['`{}`'.format(col) for col in data_dict.keys()])

    # 2. 确定值（即字典的值），并格式化为MySQL的占位符形式
    placeholders = ', '.join(['%s'] * len(data_dict))

    # 3. 构建完整的INSERT语句
    insert_statement = "INSERT INTO {} ({}) VALUES ({})".format(table_name, columns, placeholders)

    return insert_statement


conncetion = mysql.connector.connect(
    host="127.0.0.1", user="root", password="qqaazz321", database="stock"
)

cursor = conncetion.cursor(dictionary=True)

sql = """
    select * from stock where date = "2024-02-23";	
"""

cursor.execute(sql)

stockList = cursor.fetchall()
for row in stockList:
    sql = dict_to_mysql_insert("stock_py", row)
    print(sql)
    cursor.execute(sql, tuple(row.values()))
conncetion.commit()
cursor.close()
conncetion.close()
