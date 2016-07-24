__author__ = 'kevin'

def collection():
    # list
    l1 = []
    l1.append(1)
    l1.append(2)
    l2 = ['c','d']
    new_list = l1 + l2

    print new_list.__str__()

    print tuple(l1)

    # tuple
    t1 = ('a','b',3)
    print t1.__str__()
    print list(t1)

    # iter
    for a in iter(t1):
        print a

    # dict
    d1 = {1:'a',
          2:'b'}
    for k in d1.keys():
        print k,d1[k]

def tup():
    tup = ('d')
    tup.__add__('a')
    tup.__add__('f')
    print tup

def test_set():
    s = set(['1','2','3'])
    print s.__contains__('1')

def test_dic():
    arr = ['a','b','a','c']
    dic = {}
    for k in arr:
        if dic.has_key(k):
            dic[k]+=1
        else:
            dic[k]=1
    print dic

def test_arr():
    day = "20150731"
    print day[0:7]

def test_list():
    l = [1,2,3,4,'a','b','c',5]
    print(l[1:3])
def list_intersection():
    l1 = ['a','b','c','d']
    l2 = ['a','b','e','f']
    ret = set(l1).intersection(set(l2))
    print(ret)
    ret = set(l1).difference(set(l2))
    print(ret)
    ret = set(l1).union(set(l2))
    print(ret)


if __name__ == '__main__':

    #collection()
    #tup()
    #test_set()
    #test_dic()
    #test_arr()
    #test_list()
    list_intersection()
    pass