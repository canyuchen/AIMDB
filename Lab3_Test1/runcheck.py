# -*- coding:utf-8 -*-
#
#   1. aimdb,产生的结果文件以 TQn.tab命名
#   2. postgres,产生的结果文件为标准文件，以TQn.tbl命名
#   3. 命令行，python runcheck
#   4. 结果给出总共运行正确的样例个数和总样例运行个数
#   5. 如果数据出错，给出出错的数据，供查验
#

import os,sys,re

def mycmp (aaa, bbb):
    aa = aaa.split ('|')
    bb = bbb.split ('|')
    num = len (bb)
    for ii in range (0, num):
        a = aa[ii]
        b = bb[ii]
        if re.search ('\.',a):
            fa = float(a)
            fb = float(b)
            if fa < fb:
                return -1
            elif fa > fb:
                return 1
        else:
            if a < b:
                return -1
            elif a > b:
                return 1
    return 0

def myequal (aaa, bbb):
    aa = aaa.split ('|')
    bb = bbb.split ('|')
    num = len (bb)
    for ii in range (0, num):
        a = aa[ii]
        b = bb[ii]
        if re.search ('\.',a):
            fa = float(a)
            fb = float(b)
            if abs(fa-fb)/fb > 0.001:
                print 'your result:',aaa
                print 'standard result:',bbb
                return 0
        else:
            if a != b:
                print 'your result:',aaa
                print 'standard result:',bbb
                return 0
    return 1

def check (result_a, result_p):
    if not os.path.exists(result_a):
        return False;
    # 1 read files and test row number
    lines_aimdb = open (result_a,'r').readlines()
    lines_postg = open (result_p,'r').readlines()
    if len (lines_aimdb)+3 != len (lines_postg):
        print 'your rows:',len(lines_aimdb)-1,' standard rows:',len(lines_postg)-4
        print 'error: number of total rows'
        return False
    else:
        print 'rows number:',len(lines_aimdb)-1
    lines_aimdb = lines_aimdb[ :-1]
    lines_postg = lines_postg[2:-2]
    # 2 replace ' ' ->  '', replace '\t' -> '|'
    tmp_aimdb = []
    tmp_postp = []
    for ss in lines_aimdb:
        ss = '|'.join(ss.split('\t'))
        ss = ''.join(ss.split(' '))
        tmp_aimdb.append (ss[:-1])
    for ss in lines_postg:
        ss = ss.replace(' ','')
        tmp_postp.append (ss[:-1])
    lines_aimdb = tmp_aimdb
    lines_postg = tmp_postp
    #print lines_aimdb
    # 3 sort by my defined cmp fuction
    lines_aimdb.sort (mycmp)
    lines_postg.sort (mycmp)    
    # 4 compare results
    rownum = len (lines_postg)
    for ii in range (0, rownum):
        if not myequal (lines_aimdb[ii], lines_postg[ii]):
            return False
    return True

def checks (aim_dir, post_dir):
    post = os.listdir (post_dir)
    total = len (post)
    aim = 0
    test_set = [1,2,6,7,11,16,18,21]
    for item in test_set:
        pp = post_dir+'TQ'+str(item)+'.tbl'
        aa = aim_dir+'TQ'+str(item)+'.tab'
        if check(aa, pp):
            print 'TQ'+str(item)+' pass!'
            aim = aim + 1
        else:
            print 'TQ'+str(item)+' fail!'
    	print '----------------------------------'
    return aim,total

if __name__ == '__main__':
    os.system('cp ./reference/runaimdb_s.cc ../AIMDB/system/runaimdb.cc')
    os.chdir ('../AIMDB/')
    os.system('make')
    os.chdir ('../Lab3_Test1/')
    os.system('../AIMDB/runaimdb ./data/tpch_schema_small_r.txt ./data/sf10Mt/')
    aim,total = checks ('./result/aimdb_result/','./result/post_result/')
    print 'Pass :',aim
    print 'Total:',total
    pass
