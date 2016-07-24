import re
import dateutil.parser
import time
from datetime import timedelta

def extract(str=''):
    m=re.search(r'ObjectID\((?P<d>\w+)\)', str)
    if m:
        return m.group('d')
    else:
        return None

def read_write(input,output):

    dfile=open(output,'a')
    lines=[]
    _timedelta=timedelta(hours=8)
    ifile =open(input,'r')
    with ifile as f:
        alist = f.read().splitlines(True)
        for line in alist:
            try:
                _array=line.split(',')

                item5 = time.mktime((dateutil.parser.parse(_array[6])+_timedelta).timetuple())*1000
                item6 = time.mktime((dateutil.parser.parse(_array[7])+_timedelta).timetuple())*1000
                itemarray=['portal',extract(_array[0]),_array[2],_array[3].replace('"',''),_array[4].replace('"',''),_array[5].replace('"',''),str('%i' % item5),str('%i' % item6),_array[8],_array[9]]
                lines.append('\t'.join(itemarray))
            except Exception,e:
                print e.__str__()
                print(_array)

    dfile.writelines(lines)
    ifile.close()
    dfile.close()

if __name__ == '__main__':
    ymd_list=[]

    for i in range(3,20,1):
        ymd = "201510"
        if i<10:
            ymd +="0"+str(i)
        else:
            ymd += str(i)
        ymd_list.append(ymd)

    input = "auth_%s.csv"
    #ouput = "/logs_origin/back/back-portal-authorizedlog/%s/%s"
    for day in ymd_list:

        in_file = input % day
        out_file = day
        #print in_file
        read_write(in_file,out_file)
    print "over"

