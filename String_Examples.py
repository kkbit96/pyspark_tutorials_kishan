# Check if two strings are anagrams
def anagram(str1, str2):
    return sorted(str1) == sorted(str2)
# Check if two strings are anagrams using hashing
def are_anagrams(s1, s2):
    if len(s1)!=len(s2):
        return False
    count = [0]*256
    for i in range(len(s1)):
        count[ord(s1[i])] += 1
        count[ord(s2[i])] -= 1
    for x in count:
        if x!=0:
            return False
        return True

# Reverse each string of a list
def reverse_string(li):
    return [i[::-1] for i in li]
# Reverse each string of a list using map
def reverse_string2(li):
    return list(map(lambda x: x[::-1], li))