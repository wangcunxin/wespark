# -*- coding: utf-8 -*-
import random

import types
import datetime
from hanzi2pinyin import hanzi2pinyin


def test_hanzi2pinyin():
    py = hanzi2pinyin(u"上海第一妇婴保健院（东院）")
    print(py)
    py = hanzi2pinyin(u"贝联科技WiFi事业部（寰创）")
    print(py)


def stringSplit():
    s1 ="portal  242     660BF82AFD      0       15981678988     1417507353000   1417510756000   14:f6:5a:80:7a:20     172.100.10.215  Xiaomi_2013061_TD/V1 Linux/3.4.5 Android/4.2.1 Release/09.18.2013 Browser/AppleWebKit534.30 Mobile Safari/534.30 MBBMS/2.2 System/Android 4.2.1 XiaoMi/MiuiBrowser/1.0      7540149 680585  Android Miui    未知    20141203        MOBILE  1"
    arr = s1.split("\t")
    print arr
    print arr[8]
    print arr.__len__()


def re():
    import re
    strs = "how^ much for{} the maple syrup? $20.99? That's[] ricidulous!!!"
    print strs
    nstr = re.sub(r'[?|$|.|!|a|b]', r' ', strs)
    # i have taken special character to remove but any #character can be added here
    print nstr
    nestr = re.sub(r'[^a-zA-Z0-9 ]', r'', nstr)
    # for removing special character
    print nestr
    str = u"四川省妇幼保健院（抚琴院区）"
    py = re.sub(r'[(|)]', r' ', str)
    print py


def test_str():
    s1 = "zxx"
    s2 = "xx"
    s3 = "yy"
    print cmp(s1, s2)
    print cmp(s2, s3)

    print s1.capitalize()
    print s1.upper()

    str1 = "010"
    str1_2 = str1.center(10, '=')
    print str1_2
    print str1.zfill(10)

    str2 = "000wwwwb999xxx"
    in2 = str2.find("b")
    print in2
    print str2.find("m")

    str3 = "   xxoo"
    print str3.lstrip()

    print str3.ljust(20, '$')
    print str3.rjust(20, '$')


def test_int():
    i = "1440483514797"
    i2 = int(i) / 1000
    print i2

    print datetime.datetime.fromtimestamp(i2)


def test_encode():
    s = '公平'
    u = u'合理'
    # str->unicode
    u1 = s.decode('utf-8')
    print(u1)
    # unicode ->str
    s2 = u.encode('utf-8')
    print(s2)

    #print(u1+s2)

def test_unicode():
    i = '1'
    u = u'1'
    print(int(i))
    print(int(u))

    u = u"xx\tyy"
    print(u.split('\t'))

    s = "xx\tyy"
    print(s.split('\t'))

def test_print():
    s = r"\n"
    print s
    s = R"\n"
    print s

def test_compare():
    a = 100
    b = 20
    c = 3
    if a<b<c:
        print a,b
    elif a>b<c:
        print b,a
    else:
        print c

def test_type():
    a = [1]
    if type(a)==types.ListType:
        print(a)

    u = u'x'
    u = 'y'
    t = type(u)
    if t==types.UnicodeType:
        print u.encode('utf-8')+'-unicode'
    elif t==types.StringType:
        print u+'-str'
    else:
        print u+'-other'

    pass

def replaceall():
    s = '"D264214091","2014-05-19",578,236,92,144,"N,"N,"N,"N,"N,"N,54'
    print s.replace("\"",'')

def escape():
    _sql = "%s 'alter table %s add if not exists partition (dat = '%s')'"
    _sql = _sql % ('x','tb','20151111')
    print _sql

    str = "xxx %s %d" % ('yy',100)
    print str

def th_mu():
    #  a if exper else b
    i = 1
    v = False if i<0 else True
    print v

def test_range():
    print int(random.random()*100)
    for i in range(10):
        print i

def test_and():
    print True & False
    print True and False

def test_format():
    insert_sql = 'INSERT INTO `bblink_data`.`login_flow_count` (`hos_id`,`supp_id`,`date`)VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE hos_id=%s,supp_id=%s,date=%s'
    t =(u'137', u'1', u'20151212', u'137', u'1', u'20151212')
    print insert_sql % t

def test_visit_week():
    rets = []
    arr = [u'1-prelogin_20151201', u'2-*-login_20151201', u'2-*-login_20151202', u'2-olduser-login_20151201',u'3-*-forward_20151201',  u'5-*-arrive_20151201']
    s1 = "2-*-login"
    s2 = "3-*-forward"
    s3 = "5-*-arrive"
    r1=0
    r2=0
    r3=0
    for str in arr:
        a = str.split('_')
        logtype = a[0]
        if logtype==s1:
            r1+=1
        elif logtype==s2:
            r2+=1
        elif logtype==s3:
            r3+=1
    print r1,r2,r3

def substr():
    print len('A0:18:28:0E:84:35')
    ar = 'fcfc48e04d15'
    print len(ar)
    #a = list(ar)
    ret = [ar[0]]
    for i in range(1,len(ar)):
        if i%2==0:
            ret.append(":")
        ret.append(ar[i])
    print ''.join(ret)


def bianma():
    str = "78:9f:70:33:98:8b"
    print(len(str))
    str = "2014-12-02 16:05:24"
    print(len(str))
    str = "18577867225"
    print(len(str))

    s=u"中文"
    if isinstance(s, unicode):
    #s=u"中文"
        print s.encode('utf-8')
    else:
    #s="中文"
        print s.decode('utf-8').encode('gb2312')

if __name__ == '__main__':
    #test_hanzi2pinyin()
    # load_gwid_hosid()
    # re()
    # test_str()

    # stringSplit()
    # test_int()
    #test_encode()

    #test_unicode()
    #test_print()
    #test_compare()
    #test_type()
    #replaceall()

    #escape()

    #th_mu()
    #test_range()
    #test_and()
    #test_format()
    #test_visit_week()
    #substr()
    bianma()
    pass
