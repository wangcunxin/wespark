#coding:utf-8
__author__ = 'kevin'
if __name__ == '__main__':
    file = '/home/kevin/workspaces/macs.csv'
    f = open(file,'r')
    result = list()
    for ar in open(file):
        line = f.readline()
        ar =line.strip('\r\n').upper()
        ret = []
        r = None
        if len(ar)==12:
            ret = [ar[0]]
            for i in range(1,len(ar)):
                if i%2==0:
                    ret.append(":")
                ret.append(ar[i])
            r = ''.join(ret)
            result.append(r)
        '''
        elif len(ar)==17:
            r = ar
        '''

    #print result
    f.close()
    open('/home/kevin/workspaces/result.txt', 'w').write('%s' % '\n'.join(result))
