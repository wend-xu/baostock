
def clear(file_name):
    with open(file_name, 'w', encoding='utf-8') as file:
        # 写入一些文本
        file.write('')
        file.close()
def append(message,file_name,newLineFlag=True):
    with open(file_name, 'a', encoding='utf-8') as file:
        # 写入一些文本
        new_line = '\n' if newLineFlag else ''
        file.write(f'{message}{new_line}')
        file.close()
