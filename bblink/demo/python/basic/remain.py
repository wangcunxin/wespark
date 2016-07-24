import datetime

__author__ = 'kevin'

def days_add(dat,num):
    ret = None
    d1 = datetime.datetime.strptime(dat,"%Y%m%d")
    pro_time = d1 + datetime.timedelta(days=num)
    ret = pro_time.strftime("%Y%m%d")
    return ret

def compute(days,dat):
    rets = []

    for day in days:
        new_day = days_add(dat,day)
        rets.append(new_day)
    return rets

if __name__ == '__main__':
    '''
    0.cols: 1 year
    1.compute dates
    2.join sql
    3.get ds
    '''
    days = [1,2,3,4,5,6,7,10,14,21,30,60,90,120,150,180,210,240,270,300,330,360]
    dats =['20150101','20150102']
    if len(dats)>1:
        for dat in dats:
            print dat
            dates = compute(days,dat)
            print(dates)

