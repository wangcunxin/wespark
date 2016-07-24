# -*- coding: utf-8 -*-

__author__ = 'kevin'

SEP = '-'

# 基于自然周的算法,误差比基于天的算法大，所以弃用
class Arithmetic():
    '''
     0.每周连续:(weekNum[index] + weekNum[index+2])==weekNum[index+1]*2
     1.每周登录大于3:0 & mac_week,count>=3
     2.每2周登录1次:
     3.每1周登录1次:0&count=1
     4.每月(4周)登录1次:
     5.首次登录:drop
    '''

    # compute series weeks
    def series_week(self, dic):
        # {1:[1,2,3],45:[48,49],65:[70]}
        map = {}
        for k in dic.keys():
            v = dic[k]
            diff = v - k
            if map.has_key(diff):
                map.get(diff).append(v)
            else:
                map[diff] = [v]

        ret = []
        # [1-3,48-89,70-70]
        for vs in map.values():
            min = vs[0]
            max = vs[len(vs) - 1]
            ret.append(str(min) + SEP + str(max))

        return ret

    # check count
    def check_count_morethan(self, week_count_list, num):
        ret = True
        # ['2014-48-1']
        for week_count in week_count_list:
            count = int(week_count.split(SEP)[2])
            if count < num:
                ret = False
                break
        return ret

    # check count
    def check_count_equal(self, week_count_list, num):
        ret = True
        # ['2014-48-1']
        for week_count in week_count_list:
            count = int(week_count.split(SEP)[2])
            if count != num:
                ret = False
                break
        return ret

    # check weeks continuous
    def check_continue_weeks(self, week_min_max_list, num):
        ret = True
        # ['48-49','100-123']
        for min_max in week_min_max_list:
            arr = min_max.split(SEP)
            min = int(arr[0])
            max = int(arr[1])
            minus = max - min
            if minus < num:
                ret = False
                break

        return ret

    # check weeks interval
    def check_interval_weeks(self, week_min_max_list, num):
        ret = True
        # ['1-1','3-3','5-5']
        size = len(week_min_max_list)
        if size < 2:
            ret = False
        else:
            for i in range(size - 1):
                a1 = week_min_max_list[i]
                a2 = week_min_max_list[i + 1]

                if not (int(a1[0]) == int(a1[1]) & int(a2[0]) == int(a2[1])
                    & (int(a2[0]) - int(a1[0]) == num)):
                    ret = False
                    break

        return ret

    # 0.每周连续
    def continue_perweek(self, weeks):

        return len(weeks) == 1

    # 1.每周登录大于3:len(series_week)==1 & count>=3
    def perweek_morethan_3(self, weeks, counts):

        return self.continue_perweek(weeks) & self.check_count_morethan(counts, 3)

    # 3.每1周登录1次
    def perweek_equal_1(self, weeks, counts):

        return self.continue_perweek(weeks) & self.check_count_equal(counts, 1)

    # 2.每2周登录1次:1,3,5=>['1-1','3-3',...]
    def per2weeks_equal_1(self, weeks, counts):

        return self.check_interval_weeks(weeks, 2) & self.check_count_equal(counts, 1)

    # 4.每月(4周)登录1次:
    def per4weeks_equal_1(self, weeks, counts):

        return self.check_continue_weeks(weeks, 4) & self.check_count_equal(counts, 1)
