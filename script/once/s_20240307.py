import mysql.connector

conncetion = mysql.connector.connect(
    host="127.0.0.1", user="root", password="qqaazz321", database="stock"
)

cursor = conncetion.cursor(dictionary=True)

sql1 = """
select DISTINCT code from bs_stock_data_k_temp ;
"""

sql2 = """
select DISTINCT code from bs_stock_data_day_k ;
"""

cursor.execute(sql1)
sql1R = cursor.fetchall()

cursor.execute(sql2)
sql2R = cursor.fetchall()
sql2R2Dict = {r2["code"]: r2 for r2 in sql2R}

for r1 in sql1R:
    r1Code = r1["code"]
    if(sql2R2Dict.get(r1Code) == None):
        print(r1Code)
