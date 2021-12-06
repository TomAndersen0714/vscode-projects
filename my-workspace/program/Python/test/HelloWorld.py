#!python3
from collections.abc import Generator, Iterable, Iterator

if __name__ == '__main__':
    gen = (x for x in range(1, 4))
    l = [x for x in range(1, 4)]
    print(isinstance(l, list))
    print(isinstance(gen, Iterable))
    print(isinstance(gen, Iterator))
    print(isinstance(gen, Generator))
    print("Hello World!")
