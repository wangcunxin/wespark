import datetime
import subprocess

__author__ = 'kevin'

import os
import time
import paramiko


class SftpUtil:
    def __init__(self, host='', port=22, user_name='', password=''):
        self.__host = host
        self.__port = port
        self.__user_name = user_name
        self.__password = password

    def download(self, src='', dest=''):
        transport = paramiko.Transport((self.__host, self.__port))
        transport.connect(username=self.__user_name, password=self.__password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get(src, dest)
        transport.close()


class ShellUtil:
    @staticmethod
    def call(shell=''):
        return subprocess.Popen(shell, stdout=subprocess.PIPE, shell=True).stdout.read()

    @staticmethod
    def callcheck(shell=''):
        result=True
        try:
            subprocess.check_call(shell, stdout=subprocess.PIPE, shell=True)
        except Exception as e:
            print(e)
            result=False
        return result

class DateUtil:
    @staticmethod
    def get_ymdhms_now():
        now = datetime.datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        return now_str

    @staticmethod
    def get_ymd_now():
        now = datetime.datetime.now()
        #now_str = now.strftime("%Y%m%d%H%M%S")
        now_str = now.strftime("%Y%m%d")
        return now_str

    @staticmethod
    def date_to_str(date):
        date_str = None
        try:
            # s = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S.%f")
            date_str = datetime.datetime.strftime(date, "%Y-%m-%d")
        except Exception, e:
            print(e.__str__())
        return date_str

    @staticmethod
    def str_to_date(date_str):
        date = None
        try:
            # s = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S.%f")
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except Exception, e:
            print(e.__str__())
        return date

    @staticmethod
    def date_diff(x, k):
        dateStart = None
        dateEnd = None

        if x != None and x != '' and x != 'NULL':
            dateStart = DateUtil.str_to_date(x)
        if k != None and k != '' and k != 'NULL':
            dateEnd = DateUtil.str_to_date(k)

        if dateStart != None and dateEnd != None:
            return (dateStart - dateEnd).days
        else:
            return 0

    @staticmethod
    def timestamp_diff(x, k):
        dateStart = None
        dateEnd = None

        if x != None and x != '' and x != 'null':
            dateStart = DateUtil.str_to_date(x)
        if k != None and k != '' and k != 'null':
            dateEnd = DateUtil.str_to_date(k)

        if dateStart != None and dateEnd != None:
            return (dateEnd - dateStart).seconds
        else:
            return 0

    def mill_date_str(self,timeStamp):
        mill = long(timeStamp/1000)
        dateArray = datetime.datetime.utcfromtimestamp(mill)
        ret = dateArray.strftime("%Y-%m-%d")
        return ret