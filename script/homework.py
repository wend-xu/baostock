import re
# 1.将字符串“Phone: 13500124321”中的手机号字符全部隐藏。截图。
q1_text = "Phone: 13500124321"
q1_hidden_text = re.sub(r'\d', '*', q1_text)
print(q1_hidden_text)

# 2.将字符串“H E L L O”中的空格字符全部删除掉。截图。
q2_text = "H E L L O"
q2_no_space_text = re.sub(r'\s', '', q2_text)
print(q2_no_space_text)

# 3.建立字符串列表[‘abc’, ‘abc13’, ‘123’, ‘acop’, ‘zelod’, ‘556’]，筛选出仅包含数字字符的字符串。截图。
string_list = ['abc', 'abc13', '123', 'acop', 'zelod', '556']
numbers_only = [s for s in string_list if re.match(r'^\d+$', s)]
print(numbers_only)

# 4.建立字符串列表[‘abc’, ‘abc13’, ‘123’, ‘acop’, ‘zelod’, ‘556’, ‘AZS’]，筛选出同时包含字母字符和数字字符的字符串。截图。
string_list = ['abc', 'abc13', '123', 'acop', 'zelod', '556', 'AZS']
contains_alphanumeric = [s for s in string_list if re.match(r'^(?=.*[a-zA-Z])(?=.*\d).+$', s)]
print(contains_alphanumeric)

# 5.建立字符串'abc def abcd ab-  defg -bc abdc'，筛选出以a开头的关键字。截图。
text = 'abc def abcd ab-  defg -bc abdc'
starts_with_a = re.findall(r'\ba\w*\b', text)
print(starts_with_a)

# 6.建立字符串'abc def abcd ab-  defg -bc abdc'，筛选出以c结尾的关键字。截图。
text = 'abc def abcd ab-  defg -bc abdc'
ends_with_c = re.findall(r'\b\w*c\b', text)
print(ends_with_c)

# 7.建立字符串'abc def abcd ab-  defg -bc abdc oxa'，筛选出以a开头并且以c结尾的关键字。截图。
text = 'abc def abcd ab-  defg -bc abdc oxa'
keywords = re.findall(r'\ba[a-zA-Z]*c\b', text)
print(keywords)

# 8.有多道选择题及其作答做成的字符串数据“1. oooo    (A)   A.o  B.o  C.o  D.o     2. oooo    (B)   A.o  B.o  C.o  D.o”，请提取题目编号及其作答。截图。
data = "1. oooo (A) A.o B.o C.o D.o 2. oooo (B) A.o B.o C.o D.o"
questions_and_answers = re.findall(r'(\d+)\. .*? \(([A-D])\)', data)
for question, answer in questions_and_answers:
    print(f"问题编号: {question}, 作答: {answer}")

# 9.有多道选择题及其作答做成的字符串数据“1. oooo    (A)   A.o  B.o  C.o  D.o     2. oooo    (b)   A.o  B.o  C.o  D.o”，有考生在作答时不写标准大写字母而用了小写字母，请提取题目编号及其标准大写字母作答（提示lower()函数和upper()函数）。截图。
data = "1. oooo (A) A.o B.o C.o D.o 2. oooo (b) A.o B.o C.o D.o"
questions_and_answers = re.findall(r'(\d+)\. .*? \(([a-zA-Z])\)', data)
for question, answer_lower in questions_and_answers:
    answer_upper = answer_lower.upper()
    print(f"问题编号: {question}, 标准大写作答: {answer_upper}")

# 10.有多个用户信息，以(用户, 手机号)格式存储，请隐匿手机号信息，如将13580601234隐藏为135****1234。请模拟该场景，然后截图。
user_phone_data = [
    ('User1', '13580601234'),
    ('User2', '13677778888'),
    ('User3', '13712345678')
]

hidden_phone_data = [(user, re.sub(r'(\d{3})\d*(\d{4})', r'\1****\2', phone)) for user, phone in user_phone_data]
for user, hidden_phone in hidden_phone_data:
    print(f"用户: {user}, 手机号: {hidden_phone}")
