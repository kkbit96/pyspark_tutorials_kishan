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