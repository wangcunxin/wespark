# coding=utf-8
import re

class UrlParser(object):

    def __get_version(self, url=''):
        m = re.search(r'.bblink.cn/(?P<version>(v2/|v3/))', url)
        if m:
            version = m.group('version')
            if version == 'v2/':

                return 'v2'
            elif version == 'v3/':
                return 'v3'
        else:
            m = re.search(r'/(?P<version>(v2/|v3/))', url)
            if m:
                version = m.group('version')
                if version == 'v2/':

                    return 'v2'
                elif version == 'v3/':
                    return 'v3'

            return 'v1'

    def __suppId(self, url=''):
        # m=re.search(r'/(list|toUrl|content|adv|activity|linkDownApp|tools|forward|more)/(?P<supply>\d)-(?P<gwid>\w+)', url)
        m = re.search(r'/(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))', url)
        if m:
            return m.group('supply')
        else:
            return ''

    def __gwId(self, url=''):
        # m=re.search(r'/(list|toUrl|content|adv|activity|linkDownApp|tools|forward|more)/(?P<supply>\d)-(?P<gwid>\w+)', url)
        m = re.search(r'/(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))', url)
        if m:
            return m.group('gwid')
        else:
            return ''

    def __init_tools(self):
        self.dict_module = {'tag_key': '', 'supp_id': '', 'gw_id': '', 'portal_version': '', 'from_page': '', \
                            'channel_id': '', 'channel_list_id': '', 'content_id': '', 'advid': '', 'appid': '' \
                            }

    def init_base_module(self, url=''):
        self.__init_tools()
        self.dict_module['portal_version'] = self.__get_version(url)
        self.dict_module['supp_id'] = self.__suppId(url)
        self.dict_module['gw_id'] = self.__gwId(url)

    def find_no_module(self):
        return self.dict_module

    # 首页模块
    def find_index_module(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/index.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/index.html', url)
        if m:
            self.dict_module['tag_key'] = 'homepage'
            return self.dict_module
        else:
            return None

    # 怀孕知识模块
    def findPregnantModule(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/pregnant_cjg/index.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/pregnant_cjg/index.html', url)
        if m:
            self.dict_module['tag_key'] = 'cjg'
            return self.dict_module
        else:
            return None

    # 情感交流模块
    def findEmotionalModule(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/emotional/53/55.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/emotional/53/55.html$', url)
        if m:
            self.dict_module['tag_key'] = 'qgjl'
            return self.dict_module
        else:
            return None

    # 热点推荐模块
    def findHotspotModule(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/hotspot/58.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/hotspot/58.html', url)
        if m:
            self.dict_module['tag_key'] = 'rdtj'
            return self.dict_module
        else:
            return None

    # 医院列表模块
    def findHosListModule(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/hos/list.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/hos/list.html', url)
        if m:
            self.dict_module['tag_key'] = 'yylb'
            return self.dict_module
        else:
            return None

    # 同城板块栏目
    def findEmotionalTCBKModule(self, url=''):
        #dictModule = self.initBaseMoudle(url)

        # http://hoswifi.bblink.cn/v3/0-B/emotional/53/54.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/emotional/53/54.html$', url)

        if m:
            self.dict_module['tag_key'] = 'tcbk'
            return self.dict_module
        else:
            return None

    # 败品秀栏目 类同于情感交流


    # 夫妻物语
    def findEmotionalFQWYModule(self, url=''):
        #dictModule = self.initBaseMoudle(url)

        # http://hoswifi.bblink.cn/v3/0-B/emotional/53/56.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/emotional/53/56.html$', url)

        if m:
            self.dict_module['tag_key'] = 'fqwy'
            return self.dict_module
        else:
            return None

    # 婆婆妈妈
    def findEmotionalPPMMModule(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/emotional/53/57.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/emotional/53/57.html$', url)

        if m:
            self.dict_module['tag_key'] = 'ppmm'
            return self.dict_module
        else:
            return None

    # 首页焦点图
    def findIndexFocus(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/topic/55-17457.html?from=home&adv=288
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/topic/(?P<channelIdV3>\d+)-(?P<contentIdV3>\d+).html\?from=home&adv=(?P<advid>\d+)',
            url)
        if m:
            self.dict_module['tag_key'] = 'index_focus'
            self.dict_module['content_id'] = m.group('contentIdV3')
            self.dict_module['from_page'] = 'advindexfocus'
            self.dict_module['advid'] = m.group('advid')

            return self.dict_module
        else:
            return None

    # 情感交流焦点图
    def findEmotionalQGJLFocus(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/topic/55-18455.html?from=emotional&adv=311
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/topic/(?P<channelIdV3>\d+)-(?P<contentIdV3>\d+).html\?from=emotional&adv=(?P<advid>\d+)',
            url)
        if m:
            self.dict_module['tag_key'] = 'qgjl_focus'
            self.dict_module['content_id'] = m.group('contentIdV3')
            self.dict_module['from_page'] = 'advqgjlfocus'
            self.dict_module['advid'] = m.group('advid')

            return self.dict_module
        else:
            return None

    # 热门推荐焦点图
    def findRMTJFocus(self, url=''):
        #dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/video/60-18488.html?pid=60&from=hotspot&adv=313
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/video/(?P<channelIdV3>\d+)-(?P<contentIdV3>\d+).html\?pid=60&from=hotspot&adv=(?P<advid>\d+)',
            url)
        if m:
            self.dict_module['tag_key'] = 'rmtj_focus'
            self.dict_module['content_id'] = m.group('contentIdV3')
            self.dict_module['from_page'] = 'advrmtjfocus'
            self.dict_module['advid'] = m.group('advid')

            return self.dict_module
        else:
            return None

    # 备孕指南
    def findBYZNPregnant(self, url=''):
        #dictModule = self.initBaseMoudle(url)

        # http://hoswifi.bblink.cn/v3/0-B/pregnant/cjglist/82-83.html
        # m=re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/video/(?P<channelIdV3>\d+)-(?P<contentIdV3>\d+).html\?pid=60&from=hotspot&adv=(?P<advid>\d+)', url)
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/pregnant/cjglist/(?P<channelId>\d+)-(?P<channelIdList>\d+).html$',
            url)
        if m:
            self.dict_module['tag_key'] = 'byzn'
            self.dict_module['channel_id'] = m.group('channelId')
            self.dict_module['channel_list_id'] = m.group('channelIdList')

            return self.dict_module
        else:
            return None

    # 登陆页
    def findLoginPage(self, url=''):
        # http://hoswifi.bblink.cn/welcome/0-B.html
        # http://hoswifi.bblink.cn/welcome/2-a8703ac820842e1cc5ad360a288f84fb.html?gw_id=a8703ac820842e1cc5ad360a288f84fb&mac=c8:d1:0b:3f:12:72&user_ip=172.100.11.219&refer=http://www.msftncsi.com/ncsi.txt&router_ver=2.3.2&portal_suppid=2
        m = re.search(r'.bblink.cn/welcome/(?P<supply>\d)-(?P<gwid>(B|\w{10,64})).html', url)

        if m:
            suppid = m.group('supply')
            gwid = m.group('gwid')
            self.dict_module['supp_id'] = suppid
            self.dict_module['gw_id'] = gwid
            self.dict_module['portal_version'] = 'v3'
            self.dict_module['tag_key'] = 'login_page'
            return self.dict_module
        else:
            return None

    # 内容详情from
    def findHomeContentInfo(self, url=''):
        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/topic/55-17457.html?from=home
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/topic/(?P<channelIdV3>\d+)-(?P<contentIdV3>\d+).html\?from=home$',
            url)

        if m:
            self.dict_module['tag_key'] = 'home_content_info'
            self.dict_module['content_id'] = m.group('contentIdV3')
            self.dict_module['from_page'] = 'homecontent'

            return self.dict_module
        else:
            return None

    # 情感交流内容详情每一篇内容的详情
    def findQGJLContentInfo(self, url=''):

        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/topic/55-17981.html
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/topic/(?P<channelIdV3>\d+)-(?P<contentIdV3>\d+).html$',
            url)

        if m:
            self.dict_module['tag_key'] = 'qgjl_content_info'
            self.dict_module['content_id'] = m.group('contentIdV3')
            return self.dict_module
        else:
            return None

    # 欢乐谷内容详情
    def findHLGContentInfo(self, url=''):

        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/video/60-18494.html?pid=58
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/video/(?P<channelIdV3>\d+)-(?P<contentIdV3>\d+).html\?pid=',
            url)

        if m:
            self.dict_module['tag_key'] = 'hlg_content_info'
            self.dict_module['content_id'] = m.group('contentIdV3')
            return self.dict_module
        else:
            return None

    def findAboutHosMsgList(self, url=''):
        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/hos.html?from=hoslist
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/hos.html\?from=hoslist$', url)

        if m:
            self.dict_module['tag_key'] = 'about_hos_msg_list'
            self.dict_module['from_page'] = 'abouthosmsglist'

            return self.dict_module
        else:
            return None

    def findPostContent(self, url=''):
        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/topic/53/55/post.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/topic/\d+/\d+/post.html$', url)

        if m:
            self.dict_module['tag_key'] = 'post_content_info'
            return self.dict_module
        else:
            return None

    def findUserMsgList(self, url=''):
        # http://hoswifi.bblink.cn/v3/0-B/message/index.html
        # dictModule = self.initBaseMoudle(url)

        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/message/index.html', url)
        if m:
            self.dict_module['tag_key'] = 'user_msg_list'
            return self.dict_module
        else:
            return None

    def findRoomInfo(self, url=''):

        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/hos/room.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/hos/room.html', url)
        if m:
            self.dict_module['tag_key'] = 'room_info'
            return self.dict_module
        else:
            return None

    def findHosInfo(self, url=''):

        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/hos/about.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/hos/about.html', url)
        if m:
            self.dict_module['tag_key'] = 'hos_info'
            return self.dict_module
        else:
            return None

    # 登录状态下怀孕知识
    def findLoginPregnant(self, url=''):

        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/pregnant/48.html
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/pregnant/48.html', url)
        if m:
            self.dict_module['tag_key'] = 'hyzs_info'
            return self.dict_module
        else:
            return None

    # app
    def findApp(self, url=''):
        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/goto?url=http://image.bblink.cn/201412-ee50c8db96fa4bd9a2798ff18ccdf693.apk&appId=2323
        # m=re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/goto\?url=(.*)&appId=(?P<appId>\d+)', url)
        m = re.search(r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/goto\?([\s\S]*)&appId=(?P<appId>\d+)', url)
        if m:
            self.dict_module['tag_key'] = 'app'
            self.dict_module['appid'] = m.group('appId')
            return self.dict_module
        else:
            return None

    # login forward
    def findForward(self, url=''):
        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/forward/0-B/257.html
        m = re.search(r'.bblink.cn/forward/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/(?P<advId>\d+).html', url)
        if m:
            self.dict_module['tag_key'] = 'forward_ad'
            self.dict_module['advid'] = m.group('advId')
            return self.dict_module
        else:
            return None

    def findForward2(self, url=''):

        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/goto?url=http://ma.vip.com/index.php?ac=warmup_index&code=beilian&wapid=ma_466&adv=399&from=advpage
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/goto\?([\s\S]*)&adv=(?P<advid>\d+)&from=advpage', url)
        if m:
            self.dict_module['tag_key'] = 'forward_ad'
            self.dict_module['advid'] = m.group('advid')
            return self.dict_module
        else:
            return None

    def findForwardWithWechat(self,url=''):
        #dictModule = self.initBaseMoudle(url)
        #http://hoswifi.bblink.cn/wechatforward/0-B.html?loginType=
        m = re.search(r'.bblink.cn/wechatforward/(?P<version>(|v2/|v3/))\d-(B|\w{10,64}).html', url)
        if m:
            self.dict_module['tag_key']='forward_ad'
            return self.dict_module
        else:
            return None

    def findHomeFocusIsOutLink(self, url=''):
        # dictModule = self.initBaseMoudle(url)
        # http://hoswifi.bblink.cn/v3/0-B/goto?url=${ad.advertisingLinkPath}&from=home&adv=${ad.advertisingId}
        m = re.search(
            r'.bblink.cn/(?P<version>(|v2/|v3/))\d-(B|\w{10,64})/goto\?([\s\S]*)&from=home&adv=(?P<advid>\d+)', url)
        if m:
            self.dict_module['tag_key'] = 'index_focus'
            # dictModule['from_page']='advindexfocus'
            self.dict_module['advid'] = m.group('advid')
            return self.dict_module
        else:
            return None

    '''
    #新增孕密盒子
    def findTools(self,url=''):
        dictModule = self.init_tools()
        #http://hoswifi.bblink.cn/v3/tools/list.html?version=v3&gid=0-B
        m=re.search(r'.bblink.cn/v3/tools/list.html\?version=(?P<version>(v2|v3))&gid=(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))',url)
        if m:
            dictModule['portal_version'] = m.group('version')
            dictModule['supp_id'] = m.group('supply')
            dictModule['gw_id'] = m.group('gwid')

            dictModule['tag_key']='ymhz'
            return dictModule
        else:
            return None

    #待产包清单
    #http://hoswifi.bblink.cn/v3/tools/Inventory.html?version=v3&gid=0-B
    def findToolsDCBList(self,url=''):
        dictModule = self.init_tools()
        m = re.search(r'.bblink.cn/v3/tools/Inventory.html\?version=(?P<version>(v2|v3))&gid=(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))',url)
        if m:
            dictModule['portal_version'] = m.group('version')
            dictModule['supp_id'] = m.group('supply')
            dictModule['gw_id'] = m.group('gwid')

            dictModule['tag_key']='dcblist'
            return dictModule
        else:
            return None

    #疫苗接种时间表
    #http://hoswifi.bblink.cn/v3/tools/Vaccine.html?version=v3&gid=0-B
    def findYMJZTime(self,url=''):
        dictModule = self.init_tools()
        m = re.search(r'.bblink.cn/v3/tools/Vaccine.html\?version=(?P<version>(v2|v3))&gid=(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))',url)
        if m:
            dictModule['portal_version'] = m.group('version')
            dictModule['supp_id'] = m.group('supply')
            dictModule['gw_id'] = m.group('gwid')

            dictModule['tag_key']='ymjztime'
            return dictModule
        else:
            return None

    #预产期自测
    #http://hoswifi.bblink.cn/v3/tools/Perinatal.html?version=v3&gid=0-B
    def findYCQZC(self,url=''):
        dictModule = self.init_tools()
        m = re.search(r'.bblink.cn/v3/tools/Perinatal.html\?version=(?P<version>(v2|v3))&gid=(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))',url)
        if m:
            dictModule['portal_version'] = m.group('version')
            dictModule['supp_id'] = m.group('supply')
            dictModule['gw_id'] = m.group('gwid')

            dictModule['tag_key']='ycqzc'
            return dictModule
        else:
            return None

    #宝宝体重计算
    #http://hoswifi.bblink.cn/v3/tools/BodyWeight.html?version=v3&gid=0-B
    def findBabyWeight(self,url=''):
        dictModule = self.init_tools()
        m = re.search(r'.bblink.cn/v3/tools/BodyWeight.html\?version=(?P<version>(v2|v3))&gid=(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))',url)
        if m:
            dictModule['portal_version'] = m.group('version')
            dictModule['supp_id'] = m.group('supply')
            dictModule['gw_id'] = m.group('gwid')

            dictModule['tag_key']='babyweight'
            return dictModule
        else:
            return None


    #身高预测
    #http://hoswifi.bblink.cn/v3/tools/Height.html?version=v3&gid=0-B
    def findBabyHeight(self,url=''):
        dictModule = self.init_tools()
        m = re.search(r'.bblink.cn/v3/tools/Height.html\?version=(?P<version>(v2|v3))&gid=(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))',url)
        if m:
            dictModule['portal_version'] = m.group('version')
            dictModule['supp_id'] = m.group('supply')
            dictModule['gw_id'] = m.group('gwid')

            dictModule['tag_key']='babyheight'
            return dictModule
        else:
            return None

    #宝宝血型测试
    #http://hoswifi.bblink.cn/v3/tools/BloodType.html?version=v3&gid=0-B
    def findBabyBloodType(self,url=''):
        dictModule = self.init_tools()
        m = re.search(r'.bblink.cn/v3/tools/BloodType.html\?version=(?P<version>(v2|v3))&gid=(?P<supply>\d)-(?P<gwid>(B|\w{10,64}))',url)
        if m:
            dictModule['portal_version'] = m.group('version')
            dictModule['supp_id'] = m.group('supply')
            dictModule['gw_id'] = m.group('gwid')

            dictModule['tag_key']='babybloodtype'
            return dictModule
        else:
            return None

    '''


if __name__ == '__main__':
    '''
    p = UrlParser()

    #首页及首页栏目
    print(p.find_index_module('http://hoswifi.bblink.cn/v3/0-B/index.html'))
    print(p.findPregnantModule('http://hoswifi.bblink.cn/v2/0-B/ads/dsfds.html'))
    print(p.findEmotionalModule('http://hoswifi.bblink.cn/v3/0-B/emotional/53/55.html'))
    print(p.findHotspotModule('http://hoswifi.bblink.cn/v3/0-B/hotspot/58.html'))
    print(p.findHosListModule('http://hoswifi.bblink.cn/v3/0-B/hos/list.html'))

    #同城板块栏目
    print(p.findEmotionalTCBKModule('http://hoswifi.bblink.cn/v3/0-B/emotional/53/54.html'))
    print(p.findEmotionalBPXModule('http://hoswifi.bblink.cn/v3/0-B/emotional/53/55.html'))
    print(p.findEmotionalFQWYModule('http://hoswifi.bblink.cn/v3/0-B/emotional/53/56.html'))
    print(p.findEmotionalPPMMModule('http://hoswifi.bblink.cn/v3/1-B/emotional/53/57.html'))
    #焦点图
    print(p.findIndexFocus('http://hoswifi.bblink.cn/v3/0-B/topic/55-17457.html?from=home&adv=288'))
    print(p.findIndexFocus('http://hoswifi.bblink.cn/v3/0-B/topic/56-18457.html?from=home&adv=282'))
    print(p.findEmotionalQGJLFocus('http://hoswifi.bblink.cn/v3/0-B/topic/55-18455.html?from=emotional&adv=311'))
    print(p.findRMTJFocus('http://hoswifi.bblink.cn/v3/0-B/video/60-18488.html?pid=60&from=hotspot&adv=313'))

    print(p.findBYZNPregnant('http://hoswifi.bblink.cn/v3/0-B/pregnant/cjglist/82-83.html'))
    #登录页
    print(p.findLoginPage('http://hoswifi.bblink.cn/welcome/0-B.html?ref_url=http://hoswifi.bblink.cn/v3/0-B/topic/56-18075.html'))
    print(p.findLoginPage('http://hoswifi.bblink.cn/welcome/0-B.html'))
    #内容详情
    print(p.findHomeContentInfo('http://hoswifi.bblink.cn/v3/0-B/topic/55-17457.html?from=home'))
    print(p.findQGJLContentInfo('http://hoswifi.bblink.cn/v3/0-B/topic/55-17981.html'))
    print(p.findHLGContentInfo('http://hoswifi.bblink.cn/v3/0-B/video/60-18494.html?pid=58'))

    print(p.findAboutHosMsgList('http://hoswifi.bblink.cn/v3/0-B/hos.html?from=hoslist'))
    print(p.findPostContent('http://hoswifi.bblink.cn/v3/0-B/topic/53/55/post.html'))
    print(p.findUserMsgList('http://hoswifi.bblink.cn/v3/0-B/message/index.html'))
    print(p.findRoomInfo('http://hoswifi.bblink.cn/v3/0-B/hos/room.html'))
    print(p.findHosInfo('http://hoswifi.bblink.cn/v3/0-B/hos/about.html'))
    print(p.findLoginPregnant('#http://hoswifi.bblink.cn/v3/0-B/pregnant/48.html'))
    '''

    # print(p.get_version1('/v3/1-7C361BE417/index.html'))

    '''
    http://hoswifi.bblink.cn/v3/0-B/tools/list.html 孕蜜盒子页面 及子页面 追加 设备商编号-设备ID

    首页 http://hoswifi.bblink.cn/v3/0-B/goto?url=http://a.jk.cn/bl1&from=home&adv=286  外链页面 追加 设备商编号-设备ID

    android下载  http://hoswifi.bblink.cn/v3/0-B/goto?url=http://image.bblink.cn/201412-ee50c8db96fa4bd9a2798ff18ccdf693.apk&appId=2323
    ios下载  http://hoswifi.bblink.cn/v3/0-B/goto?url=https://itunes.apple.com/us/app/ping-jian-kang-guan-jia-jian/id923920872&l=zh&ls=1&mt=8&appId=2323
    '''
    '''
    print(p.findApp('http://hoswifi.bblink.cn/v3/0-B/goto?url=http://image.bblink.cn/201412-ee50c8db96fa4bd9a2798ff18ccdf693.apk&appId=2323'))
    print(p.findApp('http://hoswifi.bblink.cn/v3/0-B/goto?url=https://itunes.apple.com/us/app/ping-jian-kang-guan-jia-jian/id923920872&l=zh&ls=1&mt=8&appId=2323'))

    print(p.findForward('http://hoswifi.bblink.cn/forward/0-B/257.html'))
    url = 'http://hoswifi.bblink.cn/welcome/2-ea884bb421c3f712e9c840d428ac01d4.html?gw_id=ea884bb421c3f712e9c840d428ac01d4&mac=10:41:7f:70:79:d2&user_ip=172.100.11.210&refer=http://v.qq.com/iframe/player.html?vid=r0152brs4nc&router_ver=2.3.0&portal_suppid=2'
    print(p.findLoginPage('http://hoswifi.bblink.cn/welcome/2-ea884bb421c3f712e9c840d428ac01d4.html?gw_id=ea884bb421c3f712e9c840d428ac01d4&mac=10:41:7f:70:79:d2&user_ip=172.100.11.210&refer=http://v.qq.com/iframe/player.html?vid=r0152brs4nc&router_ver=2.3.0&portal_suppid=2'))

    print(p.findIndexmodule(url))
    print(p.findLoginPage(url))
    url = "http://hoswifi.bblink.cn/v3/0-B/goto?url=http://www.baidu.com/&adv=1902309&from=advpage"
    print(p.findForwardFocus(url))
    '''
    '''
    url = "http://hoswifi.bblink.cn/v3/tools/list.html?version=v3&gid=0-B"
    print(p.findTools(url))

    url = "http://hoswifi.bblink.cn/v3/tools/Inventory.html?version=v3&gid=0-B"
    print(p.findToolsDCBList(url))

    url = "http://hoswifi.bblink.cn/v3/tools/Vaccine.html?version=v3&gid=0-B"
    print(p.findYMJZTime(url))

    url = "http://hoswifi.bblink.cn/v3/tools/Perinatal.html?version=v3&gid=0-B"
    print(p.findYCQZC(url))

    url = "http://hoswifi.bblink.cn/v3/tools/BodyWeight.html?version=v3&gid=0-B"
    print(p.findBabyWeight(url))

    url = "http://hoswifi.bblink.cn/v3/tools/Height.html?version=v3&gid=0-B"
    print(p.findBabyHeight(url))

    url = "http://hoswifi.bblink.cn/v3/tools/BloodType.html?version=v3&gid=0-B"
    print(p.findBabyBloodType(url))

    # url = "http://hoswifi.bblink.cn/v3/0-B/goto?url=http://www.baidu.com/&from=home&adv=1902309"
    url = "http://hoswifi.bblink.cn/v3/0-B/goto?url=http%3a%2f%2fwww.baidu.com%2f&from=home&adv=1902309"
    print(p.findHomeFocusIsOutLink(url))

    url = "http://hoswifi.bblink.cn/v3/0-B/goto?url=http://ma.vip.com/index.php?ac=warmup_index&code=beilian&wapid=ma_466&adv=399&from=advpage"
    print(p.findForward2(url))

    # url = "http://hoswifi.bblink.cn/v3/0-B/goto?url=http%3A%2F%2Fstatic2.bblink.cn%2Fapp%2Fshop_android_2b9e866692779947d89091f5f9626160_5.6.5.apk&from=home&adv=2012323"
    url = "http://hoswifi.bblink.cn/v3/0-B/goto?url=http%3A%2F%2Fstatic2.bblink.cn%2Fapp%2Fshop_android_2b9e866692779947d89091f5f9626160_5.6.5.apk&from=home&adv=2012323"
    print(p.findHomeFocusIsOutLink(url))
    '''
