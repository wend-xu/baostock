import re


def camel_to_snake(name):
    """
    Convert a string from CamelCase to snake_case.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def dict_to_mysql_insert(table_name, data_dict, need_camel_to_snake=True, exclude_fields=None):
    if exclude_fields is not None:
        for exclude_field in exclude_fields:
            print(f"排除字段：{exclude_field} ")
            data_dict.pop(exclude_field, 0)
    # 1. 确定列名（即字典的键）
    if exclude_fields is None:
        exclude_fields = []
    columns = ', '.join(
        ['`{}`'.format(camel_to_snake(col) if need_camel_to_snake else col) for col in data_dict.keys()])
    # 2. 确定值（即字典的值），并格式化为MySQL的占位符形式
    placeholders = ', '.join(['%s'] * len(data_dict))
    # 3. 构建完整的INSERT语句
    insert_statement = "INSERT INTO {} ({}) VALUES ({})".format(table_name, columns, placeholders)
    values = []
    # 遍历字典的值，并将空字符串转换为None
    for value in data_dict.values():
        if value == '':
            values.append(None)  # 空字符串转换为None
        else:
            values.append(value)  # 其他值保持不变
    return insert_statement, values


def create_table_from_dict(table_name, data_dict):
    # 创建表
    column_names = ', '.join(data_dict.keys())
    table_creation_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_names} VARCHAR(255))"
    return table_creation_query
