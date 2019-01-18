测试
====

目录说明
========

data目录下包含测试所用的数据文件，包括sf=0.01的tpch数据及其schema定义

reference目录下包括3个文件，runaimdb.cc，sqls.txt(描述SQL测试语句)文件和schema.png文件（图片）

result目录下包括post_result目录和aimdb_result目录，postgres正确的运行结果放置在post_result目录中；aimdb_result暂时留空，待放入runaimdb的运行结果

runcheck.py是自动测试脚本

本README.txt文件，介绍如何进行测试

测试步骤
========

1. 将本文件包Lab3_Test与AIMDB放置在同一目录下

2. 使用runcheck来对比aimdb_result中的产生结果和标准结果post_result，程序给出正确查询个数和总查询个数，若有错误将打印出来

测试命令
========

    $cd ./Lab3_Test2/ 
    $python runcheck.py

