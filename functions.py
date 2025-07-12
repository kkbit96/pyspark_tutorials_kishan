def decorator(func):
    def wrapper():
        print("Before function is called")
        func()
        print("After function is called")
    return wrapper

@decorator
def say_hello():
    print("Hello")

say_hello()