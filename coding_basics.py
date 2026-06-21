# Move all the zeros to the end of the array.
def move_zeros_end(arr):
    move_zeros = 0
    for i in range(len(arr)):
        if(arr[i] != 0):
            arr[move_zeros] = arr[i]
            move_zeros += 1
    for i in range(move_zeros, len(arr)):
        arr[i] = 0

# Remove duplicates from a sorted array
def remove_duplicates(arr):
    if not arr:
        return 0
    unique_index = 0
    for i in range(1, len(arr)):
        if arr[i] != arr[unique_index]:
            unique_index += 1
            arr[unique_index] = arr[i]
    return unique_index + 1

def remove_duplicates_function(arr):
    return len(set(arr))

# Longest subarray with sum k
def longest_subarray_sum(arr, k):
    dic = {}
    sum = 0
    max_length = 0
    for i in range(len(arr)):
        sum += arr[i]
        if sum == k:
            max_length = i + 1
        if sum not in dic:
            dic[sum] = i
        if sum - k in dic:
            max_length = max(max_length, i - dic[sum - k])
    return max

# Sort an array of `0s`, `1s`, and `2s`without using any sorting algorithm
def sort012(ls):
    low = 0
    mid = 0
    high = len(ls) - 1

    while mid <= high:
        if ls[mid] == 0:
            ls[low], ls[mid] = ls[mid], ls[low]
            low += 1
            mid += 1
        elif ls[mid] == 1:
            mid += 1
        else:
            ls[mid], ls[high] = ls[high], ls[mid]
            high -= 1
    return ls


# Decorators
def decorator(func):
    print("Before Decorator...")
    func()
    print("After Decorator...")

@decorator
def greet():
    print("Hello World")

greet()

def next_greater(ls):
    n = len(ls)
    res = [-1]*n
    for i in range(n):
        for j in range(i + 1, n):
            if ls[j] > ls[i]:
                res[i] = ls[j]
                break
    return res

def next_greater_stack(ls):
    n = len(ls)
    res = [-1] * n
    stack = []

    for i in range(n):
        while stack and ls[stack[-1]] < ls[i]:
            res[stack.pop()] = ls[i]
        stack.append(i)

    return res

# Find the two elements that occur only once in an array where all other elements occur twice
def find_two_unique_hashmap(arr):
    """
    Finds two unique elements using a frequency dictionary / hash map.
    Time Complexity: O(N)
    Space Complexity: O(N)
    """
    freq = {}
    for num in arr:
        freq[num] = freq.get(num, 0) + 1
    
    result = [num for num, count in freq.items() if count == 1]
    return result

def find_two_unique_xor(arr):
    """
    Finds two unique elements using bitwise XOR operations.
    Time Complexity: O(N)
    Space Complexity: O(1)
    """
    # Step 1: XOR all elements in the array.
    # The result will be (x ^ y) where x and y are the two unique elements.
    xor_sum = 0
    for num in arr:
        xor_sum ^= num
    
    # Step 2: Find a set bit (rightmost set bit) in xor_sum.
    # This bit is set in one of the unique elements and not the other.
    rightmost_set_bit = xor_sum & -xor_sum
    
    # Step 3: Divide elements into two groups and XOR them separately.
    x = 0
    y = 0
    for num in arr:
        if num & rightmost_set_bit:
            x ^= num
        else:
            y ^= num
            
    return [x, y]

if __name__ == "__main__":
    test_arr = [2, 4, 7, 9, 2, 4]
    print("Array:", test_arr)
    print("Unique elements (hashmap method):", find_two_unique_hashmap(test_arr))
    print("Unique elements (XOR method):", find_two_unique_xor(test_arr))