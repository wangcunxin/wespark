import subprocess

__author__ = 'kevin'

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

if __name__ == '__main__':
    '''
    output->txtdb->parquetdb

    impala-shell -q

    0.'su - hdfs'
    1. 'hdfs dfs -chown -R impala:impala /tmp/test'
    2. 'load data ...'
    3. 'insert into tb2 select * from tb '
    '''

    sh = '/opt/cloud/hadoop/bin/hdfs dfs -chown -R root /input/wc/01'
    result = ShellUtil.callcheck(sh)
    print result

