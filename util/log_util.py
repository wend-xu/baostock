from datetime import datetime


def log_err(message: str, uk: str, level='error'):
    uk = datetime.now().strftime("%Y_%m_%d") if uk is None else uk
    with open(f'error_{uk}.txt', 'w', encoding='utf-8') as file:
        # 写入一些文本
        file.write(f'{datetime.now()} - {message} \n')
        file.close()
