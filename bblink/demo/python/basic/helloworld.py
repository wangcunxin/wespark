# -*- coding: utf-8 -*-

__author__ = 'kevin'

import sys

global a

if __name__ == '__main__':
    str = sys.argv[0]
    print str

    str = sys.argv[1]
    print str

    print(sys.path)

    for s in sys.path:
        print s

    a="hello world"
    print a
