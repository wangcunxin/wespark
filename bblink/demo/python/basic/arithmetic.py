__author__ = 'kevin'

def series_week():
    sep = "-"
    dic = {0:1,1:2,2:3,
           3:48,4:49,
           5:70}
    # {1:[1,2,3],45:[48,49],65:[70]}
    map = {}
    for k in dic.keys():
        v = dic[k]
        diff = v-k
        if map.has_key(diff):
            map.get(diff).append(v)
        else:
            map[diff] = [v]

    print map
    ret =[]
    # [1-3,48-89,70-70]
    for vs in map.values():
        min = vs[0]
        max = vs[len(vs)-1]
        ret.append(str(min)+sep+str(max))

    print ret

def round_test():
    a = 0.2222
    f = round(a,2)
    print f
    a = round(float(7)/float(3))
    print a
    f = round(a,2)
    print f
    a = 0.33363333333333
    f = "%.3f" % a
    print f

if __name__ == '__main__':
    #series_week()

    round_test()

    pass