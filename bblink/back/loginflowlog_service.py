from bblink.back.logger import *
from bblink.back.base_service import BaseService

__author__ = 'kevin'

class NumsOverall:
    def __init__(self):
        self.prelogin_num=0
        self.prelogin_pnum=0
        self.login_num=0
        self.login_pnum=0
        self.login_click_num=0
        self.login_click_pnum=0
        self.forward_num=0
        self.forward_pnum=0
        self.preArrive_num=0
        self.preArrive_pnum=0
        self.arrive_num=0
        self.arrive_pnum=0


class LogtypeOverall:
    # overall
    _prelogin = '1-prelogin'
    _login = '2-*-login'
    _login_click='2-*-login-click'
    _forward = '3-*-forward'
    _pre_arrive = '4-*-pre-arrive'
    _arrive = '5-*-arrive'


class Logtype:
    prelogin = '1-prelogin'
    mobile_login = '2-mobile-login'
    wechat_login = '2-wechat-login'
    olduser_login = '2-olduser-login'
    wechat_forward = '3-wechat-forward'
    mobile_forward = '3-mobile-forward'
    wechat_prearrive_authfail = '4-wechat-prearrive-authfail'
    wechat_prearrive_authsuccess = '4-wechat-prearrive-authsuccess'
    wechat_arrive = '5-wechat-arrive'
    mobie_arrive = '5-mobile-arrive'
    mobile_authfail_page = '6-mobile-authfail-page'
    wechat_authfail_page = '6-wechat-authfail-page'

    wechat_open_url = '3-wechat-open-url'
    wechat_geturl = '3-wechat-geturl'
    #log_wechat_auth = 'log-wechat-auth'

    wechat_arrive_url = '5-wechat-arrive-url'
    mobile_arrive_url = '5-mobile-arrive-url'

    # update
    wechat_login_click = "2-wechat-login-click"
    mobile_login_click_sucess = "2-mobile-login-click-success"
    # success fail=new
    mobile_login_click = "2-*-mobile-login-click"
    olduser_login_click = "2-olduser-login-click"

    mobile_pre_arrive = "4-mobile-pre-arrive"
    wechat_pre_arrive = "4-wechat-pre-arrive"

    wechat_forward_change_click = "3-wechat-forward-change-click"

    # combine
    _login = '2-*-login'
    _mobile_login = '2-*-mobile-login'



class Nums:
    def __init__(self):
        self._prelogin = 0
        self._preloginP = 0
        self._mobile_login = 0
        self._mobile_loginP = 0
        self._wechat_login = 0
        self._wechat_loginP = 0
        self._olduser_login = 0
        self._olduser_loginP = 0
        self._wechat_forward = 0
        self._wechat_forwardP = 0
        self._mobile_forward = 0
        self._mobile_forwardP = 0
        self._wechat_pre_arrive = 0
        self._wechat_pre_arriveP = 0
        self._wechat_prearrive_authfail = 0
        self._wechat_prearrive_authfailP = 0
        self._wechat_prearrive_authsuccess = 0
        self._wechat_prearrive_authsuccessP = 0
        self._wechat_arrive = 0
        self._wechat_arriveP = 0
        self._mobie_arrive = 0
        self._mobie_arriveP = 0
        self._mobile_authfail_page = 0
        self._mobile_authfail_pageP = 0
        self._wechat_authfail_page = 0
        self._wechat_authfail_pageP = 0

        # detail
        self.wechat_login_click_num = 0
        self.wechat_login_click_pnum = 0
        self.wechat_forward_change_click_pnum = 0
        self.wechat_forward_change_click_num = 0
        self.wechat_authurl_num = 0
        self.wechat_authurl_pnum = 0
        self.wechat_pre_arrive_num = 0
        self.wechat_pre_arrive_pnum = 0
        self.mobile_login_click_num = 0
        self.mobile_login_click_pnum = 0
        self.olduser_login_click_num = 0
        self.olduser_login_click_pnum = 0
        self.mobile_pre_arrive_num = 0
        self.mobile_pre_arrive_pnum = 0
        self.mobile_login_click_success_num = 0
        self.mobile_login_click_success_pnum = 0

        # combine
        self._login_num = 0
        self._login_pnum = 0
        self._mobile_login_num = 0
        self._mobile_login_pnum = 0



# detail
def valuesnum(nums=None, subvalue=''):
    array = subvalue.split('_')
    logtype = array[0]
    if logtype == Logtype.prelogin:
        nums._prelogin = int(array[1])
        nums._preloginP = int(array[2])
    if logtype == Logtype.mobile_login:
        nums._mobile_login = int(array[1])
        nums._mobile_loginP = int(array[2])
    if logtype == Logtype.wechat_login:
        nums._wechat_login = int(array[1])
        nums._wechat_loginP = int(array[2])
    if logtype == Logtype.olduser_login:
        nums._olduser_login = int(array[1])
        nums._olduser_loginP = int(array[2])
    if logtype == Logtype.wechat_forward:
        nums._wechat_forward = int(array[1])
        nums._wechat_forwardP = int(array[2])
    if logtype == Logtype.mobile_forward:
        nums._mobile_forward = int(array[1])
        nums._mobile_forwardP = int(array[2])
    if logtype == Logtype.wechat_pre_arrive:
        nums._wechat_pre_arrive = int(array[1])
        nums._wechat_pre_arriveP = int(array[2])
    if logtype == Logtype.wechat_prearrive_authfail:
        nums._wechat_prearrive_authfail = int(array[1])
        nums._wechat_prearrive_authfailP = int(array[2])
    if logtype == Logtype.wechat_prearrive_authsuccess:
        nums._wechat_prearrive_authsuccess = int(array[1])
        nums._wechat_prearrive_authsuccessP = int(array[2])
    if logtype == Logtype.wechat_arrive:
        nums._wechat_arrive = int(array[1])
        nums._wechat_arriveP = int(array[2])
    if logtype == Logtype.mobie_arrive:
        nums._mobie_arrive = int(array[1])
        nums._mobie_arriveP = int(array[2])
    if logtype == Logtype.mobile_authfail_page:
        nums._mobile_authfail_page = int(array[1])
        nums._mobile_authfail_pageP = int(array[2])
    if logtype == Logtype.wechat_authfail_page:
        nums._wechat_authfail_page = int(array[1])
        nums._wechat_authfail_pageP = int(array[2])
    #
    if logtype == Logtype.wechat_login_click:
        nums.wechat_login_click_num = int(array[1])
        nums.wechat_login_click_pnum = int(array[2])
    if logtype == Logtype.wechat_forward_change_click:
        nums.wechat_forward_change_click_num = int(array[1])
        nums.wechat_forward_change_click_pnum = int(array[2])
    if logtype == Logtype.wechat_geturl:
        nums.wechat_authurl_num = int(array[1])
        nums.wechat_authurl_pnum = int(array[2])
    if logtype == Logtype.wechat_pre_arrive:
        nums.wechat_pre_arrive_num = int(array[1])
        nums.wechat_pre_arrive_pnum = int(array[2])
    if logtype == Logtype.mobile_login_click:
        nums.mobile_login_click_num = int(array[1])
        nums.mobile_login_click_pnum = int(array[2])
    if logtype == Logtype.olduser_login_click:
        nums.olduser_login_click_num = int(array[1])
        nums.olduser_login_click_pnum = int(array[2])
    if logtype == Logtype.mobile_pre_arrive:
        nums.mobile_pre_arrive_num = int(array[1])
        nums.mobile_pre_arrive_pnum = int(array[2])
    if logtype == Logtype.mobile_login_click_sucess:
        nums.mobile_login_click_success_num = int(array[1])
        nums.mobile_login_click_success_pnum = int(array[2])
    #
    if logtype == Logtype._login:
        nums._login_num = int(array[1])
        nums._login_pnum = int(array[2])
    if logtype == Logtype._mobile_login:
        nums._mobile_login_num = int(array[1])
        nums._mobile_login_pnum = int(array[2])


class LoginflowlogMysqlService(BaseService):
    num = None

    def setNums(self,r):
        logtype = r[2]
        if logtype==LogtypeOverall._prelogin:
            self.num.prelogin_num=r[0]
            self.num.prelogin_pnum=r[1]
        elif logtype==LogtypeOverall._login:
            self.num.login_num=r[0]
            self.num.login_pnum=r[1]
        elif logtype==LogtypeOverall._login_click:
            self.num.login_click_num=r[0]
            self.num.login_click_pnum=r[1]
        elif logtype==LogtypeOverall._forward:
            self.num.forward_num=r[0]
            self.num.forward_pnum=r[1]
        elif logtype==LogtypeOverall._pre_arrive:
            self.num.preArrive_num=r[0]
            self.num.preArrive_pnum=r[1]
        elif logtype==LogtypeOverall._arrive:
            self.num.arrive_num=r[0]
            self.num.arrive_pnum=r[1]

    def row2column(self,day):

        ret_list = []
        tup = (day,self.num.prelogin_num,self.num.prelogin_pnum,self.num.login_num,self.num.login_pnum,
               self.num.login_click_num,self.num.login_click_pnum,self.num.forward_num,self.num.forward_pnum,
               self.num.preArrive_num,self.num.preArrive_pnum,self.num.arrive_num,self.num.arrive_pnum)
        ret_list.append(tup)

        return ret_list

    def getRetOverall(self,rs,day):
        ret_list = []
        if len(rs)>0:
            self.num = NumsOverall()
            # evaluate
            for r in rs:
                logger.debug(r)
                self.setNums(r)
            # row->column
            ret_list = self.row2column(day)
        logger.debug(ret_list)
        return ret_list

    # detail
    def getRetKeys(self,rs):
        ret_list = []
        if len(rs)>0:
            for r in rs:
                key = r[0].split('_')
                _keytuple = (key[0], key[1], key[2], key[0], key[1], key[2])
                ret_list.append(_keytuple)
        return ret_list

    def getRetValues(self,rs):
        ret_list = []
        if len(rs)>0:
            for item in rs:
                key = item[0].split('_')
                values = item[1]
                nums = Nums()
                for subvalue in iter(values):
                    valuesnum(nums, subvalue)
                _tuple = (nums._prelogin, nums._preloginP,
                          nums._mobile_login, nums._mobile_loginP,
                          nums._wechat_login, nums._wechat_loginP,
                          nums._olduser_login, nums._olduser_loginP,
                          nums._wechat_forward, nums._wechat_forwardP,
                          nums._mobile_forward, nums._mobile_forwardP,
                          nums._wechat_pre_arrive, nums._wechat_pre_arriveP,
                          nums._wechat_prearrive_authfail, nums._wechat_prearrive_authfailP,
                          nums._wechat_prearrive_authsuccess, nums._wechat_prearrive_authsuccessP,
                          nums._wechat_arrive, nums._wechat_arriveP,
                          nums._mobie_arrive, nums._mobie_arriveP,
                          nums._mobile_authfail_page, nums._mobile_authfail_pageP,
                          nums._wechat_authfail_page, nums._wechat_authfail_pageP,

                          nums.wechat_login_click_num,nums.wechat_login_click_pnum,
                          nums.wechat_forward_change_click_num,nums.wechat_forward_change_click_pnum,
                          nums.wechat_authurl_num,nums.wechat_authurl_pnum,
                          nums.wechat_pre_arrive_num,nums.wechat_pre_arrive_pnum,
                          nums.mobile_login_click_num,nums.mobile_login_click_pnum,
                          nums.olduser_login_click_num,nums.olduser_login_click_pnum,
                          nums.mobile_pre_arrive_num,nums.mobile_pre_arrive_pnum,
                          nums.mobile_login_click_success_num,nums.mobile_login_click_success_pnum,
                          nums._login_num,nums._login_pnum,
                          nums._mobile_login_num,nums._mobile_login_pnum,

                          key[0], key[1], key[2],
                          )

                ret_list.append(_tuple)
        return ret_list
