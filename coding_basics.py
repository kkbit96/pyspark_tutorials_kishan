# Move all the zeros to the end of the array.
def move_zeros_end(arr):
    move_zeros = 0
    for i in range(len(arr)):
        if(arr[i] != 0):
            arr[move_zeros] = arr[i]
            move_zeros += 1
    for i in range(move_zeros, len(arr)):
        arr[i] = 0