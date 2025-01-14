def my_decorator(func):
    def wrapper():
        print("Something is happening before the function is called.")
        func()
        print("Something is happening after the function is called.")
    return wrapper

@my_decorator
def say_hello():
    print("Hello!")

say_hello()

def restore_original_value(func):
    def wrapper(x):
        original_x = x
        result = func(x)
        return original_x
    return wrapper

@restore_original_value
def subtract_two(x):
    return x - 2

# Example usage
result = subtract_two(10)
print(result)