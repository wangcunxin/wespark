from datetime import datetime
from datetime import timedelta
import time

__author__ = 'kevin'

def test1():
    begin =datetime.datetime.now();
    time.sleep(1)
    end =datetime.datetime.now();
    print end-begin

    begin =time.time()
    time.sleep(1)
    end =time.time()
    print end-begin

def test2():
    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    print now,now_str
    now_str = now.strftime("%Y%m%d")
    print now,now_str

def test_time_long():
    t = time.time()
    print t
    print int(t)

def test_str_mill():
    # 1448888696766
    date = "20151212"

    d1 = datetime.datetime.strptime(date,"%Y%m%d")
    time_mill = int(time.mktime(d1.timetuple())*1000)
    print time_mill

def test_str_date():

    timeStamp = 1449849600000/1000
    dateArray = datetime.datetime.utcfromtimestamp(timeStamp)
    otherStyleTime = dateArray.strftime("%Y-%m-%d")

    print otherStyleTime

def test_week():
    print datetime.datetime(2006,9,4).isocalendar()[1]
    print time.strftime("%W")

    timeStamp = 1449849600000/1000
    dateArray = datetime.datetime.utcfromtimestamp(timeStamp)
    print dateArray.strftime("%Y-%W")
def test_datetime():
    arr = "2015-01-01".split("-")
    d1 = datetime.datetime(int(arr[0]),int(arr[1]),int(arr[2]))
    arr = "2016-01-01".split("-")
    d2 = datetime.datetime(int(arr[0]),int(arr[1]),int(arr[2]))
    print (d2-d1).days

def test_days():
    #def datediff(beginDate,endDate):
    beginDate="20150101"
    endDate="20150103"
    format="%Y%m%d"
    bd=datetime.strptime(beginDate,format)
    ed=datetime.strptime(endDate,format)
    ret = bd-ed
    print ret.days
    '''
    oneday=datetime.timedelta(days=-1)
    count=0
    while bd!=ed:
        ed=ed+oneday
        count+=1
    print count

    '''

def test_offset():
    DAY_OFFSET=30*8
    #--set datetime
    beginDate="2015-01-01"
    format="%Y-%m-%d";
    arr = beginDate.split("-")
    t1 = datetime.datetime(int(arr[0]),int(arr[1]),int(arr[2]))
    now = datetime.date(t1.year,t1.month,t1.day)
    pro_time=now-datetime.timedelta(days=DAY_OFFSET)
    #dest_time_str=pro_time.strftime("%Y-%m-%d")
    print pro_time

def test_None():
    longtime = 143835
    ret = datetime.datetime.fromtimestamp(long(longtime / 1000)).strftime('%Y%m%d')
    print ret

def test_days_2(beginDate,endDate):
    #def datediff(beginDate,endDate):
    #beginDate="2015-01-01"
    #endDate="2015-01-03"
    format="%Y-%m-%d";
    bd=datetime.strptime(beginDate,format)
    ed=datetime.strptime(endDate,format)
    diff = ed-bd
    #print diff.days
    return diff.days

def getMedian(a):
    ret =0
    size = len(a)
    mid = size%2
    if mid==0:
        #ret = (a[size/2]+a[size/2-1])/2
        ret = a[size/2-1]
    elif mid==1:
        ret = a[(size-1)/2]

    return ret
def test_median(ars):
    ret = []
    for ar in ars:
        m = getMedian(ar)
        ret.append(m)
    print ret
    return ret

def listSum(ar):
    ret =0
    size = len(ar)
    for i in range(0,size-1):
        ret +=test_days_2(ar[i],ar[i+1])
    return ret

def get_avg_diff(ar):
    ret =0
    size = len(ar)
    if size==2:
        ret = test_days_2(ar[1],ar[0])
    elif size>=3:
        ret = listSum(ar)/(size-1)
    return ret

def test_diff(diff_):
    _data = ['2016-01-08','2016-01-01','2016-01-03','2016-01-02','2016-01-02',
             '2016-01-11','2016-01-21','2016-01-21','2016-01-23','2016-01-24','2016-01-25']
    l1 = set(_data)
    ds = list(l1)
    list.sort(ds)
    _data = ds

    # _data=(1,2,4,9,10,11,14,18,23,27,33);
    _outerList=[]
    _tmpNum=_data[0];
    _list=[]

    for _num in _data:
        diff=test_days_2(_tmpNum,_num)
        if diff<=diff_:
            _list.append(_num)
            _tmpNum = _num
        else:
            _outerList.append(_list)
            _list=[]
            _tmpNum=_num
            _list.append(_num)

    _outerList.append(_list)
    print(_outerList)

    medians = test_median(_outerList)
    diff_avg = get_avg_diff(medians)
    print diff_avg

def compute_days(beginDate,endDate):
    format="%Y%m%d";
    bd=datetime.strptime(beginDate,format)
    ed=datetime.strptime(endDate,format)
    diff = ed-bd
    return diff.days

def timestamp():
    d = "2010-06-06"
    '''
    a = d.split("-")
    dateC=datetime(int(a[0]),int(a[1]),int(a[2]))
    timestamp=time.mktime(dateC.timetuple())
    print(long(timestamp))
    '''
    #time.strptime(d, '%Y-%m-%d')
    s= time.mktime(time.strptime(d, '%Y-%m-%d'))
    print int(s)

if __name__ == '__main__':
    #test1()
    #test2()
    #test_time_long()
    #test_str_mill()
    #test_str_date()
    #test_week()
    #test_datetime()
    #test_days()
    #test_offset()
    #test_None()
    #test_median()
    #test_diff(2)
    timestamp()

    pass
