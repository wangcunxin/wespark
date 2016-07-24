from bblink.website.sitepvv3.base_service import BaseService
from bblink.website.sitepvv3.logger import logger
from bblink.website.utils.util import DateUtil

__author__ = 'kevin'

class Sitepvv3Service(BaseService):
    # compute pv
    def exec_file(self, sql_context,begin_time,end_time):

        #
        _sql = ""
        logger.debug(_sql)
        _output =""
        rs = sql_context.sql(_sql)
        # parquet
        self._write_parquet(rs, _output)

    # transfer log
    def exec_file_transfer(self, sql_context):
        _sql = ""
        rs = sql_context.sql(_sql)
        # pv to file
        pv = rs.count()
        ymd_hms = DateUtil.get_Ymdhms_now()
        line = ymd_hms+","+pv
        output_pv = ""
        self._write_file(list(line),output_pv)