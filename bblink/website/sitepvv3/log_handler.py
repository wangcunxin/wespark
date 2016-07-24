# -*- coding: utf-8 -*-

import datetime

from user_agents import parse

from bblink.website.sitepvv3.url_handler import UrlParser

__author__ = 'kevin'


class LogHandler(object):
    __sep = "\t"

    def __init__(self):
        self.__INDEX_URL = 4
        self.__INPUT_SPLIT = '\t'
        self.__OUTPUT_SPLIT = ','
        self.__REPLACE_RES = '@'

    def __parse_user_agents(self, ua_string=''):
        user_agent = parse(ua_string)
        return user_agent

    def __timeFormat(self, date_time=0):
        if date_time > 0:
            return datetime.datetime.fromtimestamp(date_time)

    def _parse_url(self, url):
        try:
            parser = UrlParser()
            parser.init_base_module(url)

            m = parser.find_index_module(url)
            if m: return m
            m = parser.findPregnantModule(url)
            if m: return m
            m = parser.findEmotionalModule(url)
            if m: return m
            m = parser.findHotspotModule(url)
            if m: return m
            m = parser.findHosListModule(url)
            if m: return m
            m = parser.findEmotionalTCBKModule(url)
            if m: return m
            m = parser.findEmotionalFQWYModule(url)
            if m: return m
            m = parser.findEmotionalPPMMModule(url)
            if m: return m
            m = parser.findIndexFocus(url)
            if m: return m
            m = parser.findEmotionalQGJLFocus(url)
            if m: return m
            m = parser.findRMTJFocus(url)
            if m: return m
            m = parser.findBYZNPregnant(url)
            if m: return m
            m = parser.findLoginPage(url)
            if m: return m
            m = parser.findHomeContentInfo(url)
            if m: return m
            m = parser.findQGJLContentInfo(url)
            if m: return m
            m = parser.findHLGContentInfo(url)
            if m: return m
            m = parser.findAboutHosMsgList(url)
            if m: return m
            m = parser.findPostContent(url)
            if m: return m
            m = parser.findUserMsgList(url)
            if m: return m
            m = parser.findRoomInfo(url)
            if m: return m
            m = parser.findHosInfo(url)
            if m: return m
            m = parser.findLoginPregnant(url)
            if m: return m
            m = parser.findApp(url)
            if m: return m
            m = parser.findForward(url)
            if m: return m
            m = parser.findForward2(url)
            if m: return m
            m = parser.findForwardWithWechat(url)
            if m: return m
            m = parser.findHomeFocusIsOutLink(url)
            if m:
                return m
            else:
                return parser.find_no_module()
        except Exception, e:
            print(e.__str__())
            return None

    # parse url and add fields
    def handle(self, line):

        '''
        "pid", "guuid", "guuidCTime", "uuid", "uuidCTime",
        "pageTitle", "url", "referer", "language", "cookieEnabled",
        "sw", "sh", "prevPID", "prevTime"， "ua",
        "ip", "createTime"
        '''
        ret = u''

        try:
            url = line[6]
            extra_info = self._parse_url(url)

            guuidCTime = line[2]
            guuidCTimeStr = u""
            if guuidCTime != None and guuidCTime != "":
                guuidCTimeStr = self.__timeFormat(int(int(guuidCTime) / 1000))

            referer = line[7]

            createTime = line[16]
            createTimeStr = u""
            if createTime != None and createTime != "":
                createTimeStr = self.__timeFormat(long(float(createTime) / 1000))

            ua = line[14]
            browserStr = u""
            osStr = u""
            if ua != None and ua != "":
                browserStr = self.__parse_user_agents(ua).browser.family
                osStr = self.__parse_user_agents(ua).os.family

            uuidCTime = line[4]
            uuidCTimeStr = u""
            if uuidCTime != None and uuidCTime != "":
                uuidCTimeStr = self.__timeFormat(int(uuidCTime) / 1000)
            '''
                schema_string = "site_id,site_uuid,site_uuid_ctime,ptitle,url," \
                                "referrer,prevPID,attime,resolution,ip," \
                                "ctime,language,cookie_enabled,ua,uuid," \
                                "uuid_ctime,browser,os,tag_key,supp_id," \
                                "gw_id,portal_version,from_page,channel_id,channel_list_id," \
                                "content_id,advid,appid"
                '''
            result_str = u"{0}\t{1}\t{2}\t{3}\t{4}\t" \
                         "{5}\t{6}\t{7}\t{8}x{9}\t{10}\t" \
                         "{11}\t{12}\t{13}\t{14}\t{15}\t" \
                         "{16}\t{17}\t{18}\t{19}\t{20}\t" \
                         "{21}\t{22}\t{23}\t{24}\t{25}\t" \
                         "{26}\t{27}\t{28}"

            if extra_info:
                result_str = result_str.format(
                    line[0],  # pid
                    line[1],  # guuid
                    guuidCTimeStr,  # guuidCTime
                    line[5],  # pageTitle
                    url,  # url

                    referer,  # referer
                    line[12],  # prevPID
                    line[13],  # prevTime
                    line[10], line[11],  # sw * sh
                    line[15],  # ip

                    createTimeStr,  # createTime
                    line[8],  # language
                    line[9],  # cookieEnabled
                    ua,  # ua
                    line[3],  # uuid

                    uuidCTimeStr,  # uuidCTime
                    browserStr,  # browser
                    osStr,  # os
                    # fill field
                    extra_info['tag_key'],  # url tag标示
                    extra_info['supp_id'],  # 供应商id

                    extra_info['gw_id'],  # 设备id
                    extra_info['portal_version'],  # portal 版本
                    extra_info['from_page'],  # 来源页标示
                    extra_info['channel_id'],  # 频道&&栏目id
                    extra_info['channel_list_id'],  # 频道&&栏目列表id

                    extra_info['content_id'],  # 内容详情id
                    extra_info['advid'],  # 广告id
                    extra_info['appid'])  # 下载appid

            else:
                result_str = result_str.format(
                    line[0],  # pid
                    line[1],  # guuid
                    guuidCTimeStr,  # guuidCTime
                    line[5],  # pageTitle
                    url,  # url

                    referer,  # referer
                    line[12],  # prevPID
                    line[13],  # prevTime
                    line[10],  # sw
                    line[11],  # sh
                    line[15],  # ip

                    createTimeStr,  # createTime
                    line[8],  # language
                    line[9],  # cookieEnabled
                    ua,  # ua
                    line[3],  # uuid

                    uuidCTimeStr,  # uuidCTime
                    browserStr,  # browser
                    osStr,  # os
                    # fill field
                    u'',
                    u'',

                    u'',
                    u'',
                    u'',
                    u'',
                    u'',

                    u'',
                    u'',
                    u'')
            ret = result_str

        except Exception, e:
            print(e.__str__())
            pass

        return ret


if __name__ == '__main__':
    p = LogHandler()
    url = "http://hoswifi.bblink.cn/wechatforward/2-0d97fe8d363a991720a04ea5fc97fea9.html?loginType=WECHAT"
    m = p._parse_url(url)
    print m
