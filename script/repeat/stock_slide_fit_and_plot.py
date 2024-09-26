#
# flag = 0
# while flag != -1:
#     print("请输入股票编码或名称,输入-1退出")
#     input_code_or_name = input()
#     print(input_code_or_name)
#     if input_code_or_name == '-1':
#         flag = -1
#         print("退出程序")

import time

for i in range(10):
    # 使用\r将光标移动到行首，然后输出当前的内容
    print(f"进度: {i + 1}/10", end='\r')
    # 为了更好地看到效果，可以暂停一下
    time.sleep(1)

# 最后输出一个换行符，以便接下来的输出从新的一行开始
print()