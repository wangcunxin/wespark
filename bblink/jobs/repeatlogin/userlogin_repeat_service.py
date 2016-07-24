from bblink.jobs.repeatlogin.base_service import *
from bblink.jobs.repeatlogin.userlogin_repeat import *
from bblink.jobs.repeatlogin.logger import *

__author__ = 'kevin'


class UserLoginRepeatService(BaseService):
    def __get_times(self, begin, end, period):
        r = 0
        diff = DateUtil.timestamp_diff(begin, end)
        if diff <= period:
            r = 1

        return r

    def __calculate(self, list, period):
        time_list = []
        for ele in list:
            time_list.append(ele)
        size = len(time_list)

        times = 0
        if size > 1:
            for i in range(0, size - 1, 1):
                times += self.__get_times(time_list[i], time_list[i + 1], period)

        return times

    def exec_file(self, sql_context, begin, end):
        sql_ = ConfigPortalSql.select_userlogin_repeat % (begin, end)
        logger.debug(sql_)
        rs = sql_context.sql(sql_)

        key_value_list = rs.map(lambda r: (DateUtil.date_to_str(r[0]) + "," + r[1] + "," + r[2], r[3])).groupByKey(1).collect()

        logger.debug('-->rs:'+str(len(key_value_list)))

        lines_list = []
        period_list = [2, 5, 10, 30, 60]
        sep = "\t"
        for key, value in key_value_list:
            times_list = []
            for period in period_list:
                times = self.__calculate(value, period * 60)
                times_list.append(str(times))

            line_list = key.split(",") + times_list
            #logger.debug('-->line:'+str(line_list))
            line = sep.join(line_list)
            lines_list.append(line)

        logger.info('lines size:'+str(lines_list.__len__()))
        output = ConfigSparkPath.userlogin_repeat_path % begin
        logger.debug(output)

        # write to file
        self._write_file(lines_list, output)
        return lines_list

    def exec_file_other(self, sql_context,begin):
        sql_ = ConfigPortalSql.select_userlogin_repeat_sta
        logger.debug(sql_)
        rs = sql_context.sql(sql_)

        lines_list = []
        sep = "\t"
        for r in rs.collect():
            if r != None and len(r) > 0:
                tmp = []
                for t in r:
                    tmp.append(str(t))
                line = sep.join(tmp)
                #line = r[0]+sep+r[1]+sep+r[2]+sep+r[3]+sep+r[4]+sep+r[5]+sep+r[6]
                lines_list.append(line)
        logger.info('lines size:'+str(lines_list.__len__()))
        output = ConfigSparkPath.userlogin_repeat_path % begin +'_2'
        logger.debug(output)

        # write to file
        self._write_file(lines_list, output)
