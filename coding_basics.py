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