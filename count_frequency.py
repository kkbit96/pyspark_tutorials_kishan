def count_freq_words(s):
    li = s.split()
    dic = {}
    for i in li:
        if i in dic:
            dic[i] += 1
        else:
            dic[i] = 1
    return dic

# convert two lists into a dictionary
def convert_to_dic(li1, li2):
    dic = {}
    for i in range(len(li1)):
        dic[li1[i]] = li2[i]
    return dic
# Convert two lists into a dictionary using dictionary comprehension
def convert_to_dic2(li1, li2):
    dic = {li1[i]: li2[i] for i in range(len(li1))}
    return dic
# Convert two list into a dictionary using zip
def convert_to_dic3(li1, li2):
    dic = dict(zip(li1, li2))
    return dic

# Convert dictionary items into a tuple
def dic_to_tuple(dic):
    return dic.items()

# Two sum problem Find the elements in the list that sum up to the target
def two_sum(li, target):
    dic = {}
    for i in range(len(li)):
        if target - li[i] in dic:
            return [dic[target - li[i]], i]
        dic[li[i]] = i
    return []

dataList = [{'a': 1}, {'b': 3}, {'c': 5}]
print(*[val for dic in dataList for val in dic.values()], sep='\n')

use=[{'id': 29207858, 'isbn': '1632168146', 'isbn13': '9781632168146', 'ratings_count': 0}]
for dic in use:
    for val,cal in dic.items():
        print(f'{val} is {cal}')

# Merge two dictionaries
dic1 = {'a': 1, 'b': 2}
dic2 = {'c': 3, 'd': 4}
dic1.update(dic2)
print(dic1)

dict1 = {'name' : 'Alice', 'age' : 25}
for i in enumerate(dict1):
    print(i)

keys = ['a', 'b', 'c']

#Using fromkeys
my_dict = dict.fromkeys(keys, 0)
print(my_dict)

my_dict = {'name': 'Alice'}
# Adds 'age' with default value 25
value = my_dict.setdefault('age', 25)
print(my_dict)
print(my_dict.items())

# We can also remove a key pair from dictionary using pop() method as well
test_dict = {'gfg' : 4, 'is' : 7, 'best' : 10}
print(test_dict)
del test_dict['is']
print(str(test_dict))