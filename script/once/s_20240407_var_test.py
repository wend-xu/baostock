def func_with_both_1(*args, **kwargs):
    print("Positional arguments:")
    for arg in args:
        print(arg)
    print("Keyword arguments:")
    for key, value in kwargs.items():
        print(f"{key} = {value}")


def func_with_both_2(*args, **kwargs):
    func_with_both_1(*args, **kwargs)

func_with_both_2(1, 2, 3, a=4, b=5)
