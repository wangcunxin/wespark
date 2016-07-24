# -*- coding: utf-8 -*-

import jieba

if __name__ == '__main__':
    dic = "/home/kevin/workspaces/pycharm/spark_py/bblink/demo/python/basic/dic.txt"
    jieba.load_userdict(dic)
    jieba.initialize()
    #all
    seg_list = jieba.cut("我来到北京清华大学王存昕", cut_all=True)
    print('/'.join(list(seg_list)))
    #jinzhun
    seg_list = jieba.cut("我来到北京清华大学王存昕", cut_all=False)
    print('/'.join(list(seg_list)))
    #search
    seg_list = jieba.cut_for_search("我来到北京清华大学王存昕")
    print('/'.join(list(seg_list)))


    pass