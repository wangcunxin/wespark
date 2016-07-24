
# -*- coding: utf-8 -*-
__author__ = 'kevin'
import datetime

SEP = '-'


# 基于天的算法
class Arithmetic():
    '''
     :count>1
     0.每周连续:diff<7+n,1
     1.每周登录大于3:0 & diff1+diff2<7+n,1
     2.每2周只登录1次: 14+n>diff>14-n,2
     3.每1周只登录1次:7-n<diff<7+n,1
     4.每月登录1次:30-n<diff<30+n,4
     5.首次登录:count==1
    '''
    # 间隔月份前日期
    def daydiff(self,day,month):
        arr = day.split(SEP)
        t1 = datetime.datetime(int(arr[0]),int(arr[1]),int(arr[2]))
        now = datetime.date(t1.year,t1.month,t1.day)
        day_offset=30*month
        dest_time=now-datetime.timedelta(days=day_offset)
        format="%Y-%m-%d"
        return dest_time.strftime(format).decode('utf-8')
    # 间隔天数
    def datediff(self,begin,end):
        a1 = begin.split(SEP)
        a2 = end.split(SEP)
        d1 = datetime.datetime(int(a1[0]),int(a1[1]),int(a1[2]))
        d2 = datetime.datetime(int(a2[0]),int(a2[1]),int(a2[2]))

        return (d2-d1).days

    # set=date_list,days=number,deviation=+-days
    def check_continue(self,set,days,deviation):
        ret = True
        size = len(set)
        max = days+deviation
        for i in range(size-1):
            if not (self.datediff(set[i+1],set[i])<max):
                ret = False
                break
        return ret

    def check_diff_sum(self, set, days, deviation):
        ret = True
        size = len(set)
        tmp_diffs=[]
        if size>=3:
            max = days+deviation
            for i in range(size-2):
                diff = self.datediff(set[i+1],set[i])
                tmp_diffs.append(diff)
                # diff1+diff2<7+n,1
                if i>0 & (not (tmp_diffs[i-1] + diff < max)):
                    ret = False
                    break

        return ret
    # 14+n>diff>14-n,2
    def check_continue_between(self,set,days,deviation):
        ret = True
        size = len(set)
        max = days+deviation
        min = days-deviation
        for i in range(size-1):
            if not (min<self.datediff(set[i+1],set[i])<max):
                ret = False
                break
        return ret

    # 0.每周连续:diff<7+n,1
    def continue_perweek(self, set):

        return self.check_continue(set,7,1)

    # 1.每周登录大于3:0 & diff1+diff2<7+n,1
    def perweek_morethan_3(self, set):

        return self.check_continue(set,7,1) & self.check_diff_sum(set,7,1)

    # 2.每2周只登录1次: 14+n>diff>14-n,2
    def per2weeks_equal_1(self, set):

        return self.check_continue_between(set,7*2,1*2)

    # 3.每1周只登录1次:7-n<diff<7+n,1
    def perweek_equal_1(self, set):

        return self.check_continue_between(set,7*1,1*1)

    # 4.每月登录1次:30-n<diff<30+n,4
    def per4weeks_equal_1(self, set):

        return self.check_continue_between(set,30,4)

