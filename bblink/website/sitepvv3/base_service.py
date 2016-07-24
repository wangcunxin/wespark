from bblink.website.sitepvv3.logger import logger

__author__ = 'kevin'

# execute sql and save to db
class BaseService:
    def __init__(self):
        pass

    # parquet
    def _write_parquet(self, df, output_path):

        try:
            # df.show()
            logger.debug(output_path)

            df.coalesce(1).write.mode('append').parquet(output_path)
        except Exception, e:
            logger.error(e.__str__())

    # file:file:/// or hdfs://namenode:port/
    def _write_file(self, list, output_path):
        fo = None
        try:
            fo = open(output_path, 'w')
            for line in list:
                fo.write(line+'\n')

        except Exception, e:
            print(e.__str__())
        finally:
            try:
                if fo != None:
                    fo.close()
            except Exception, e:
                print(e.__str__())

