import re
str = '10sec'

lists = re.findall(r'\d+', str)
print(lists)