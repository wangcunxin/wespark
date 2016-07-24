__author__ = 'kevin'

def bytearray_string():
    b = b'outflow'
    print b
    u = 'outflow'
    if str(b) == u:
        print u
        print type(str(b))
        print type(u)

if __name__ == '__main__':
    bytearray_string()

    pass