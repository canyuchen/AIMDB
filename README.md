# AIMDB

## My Contribution :

* 继承Operator类实现具体功能的子类
* 解析SelectQuery生成QueryPlan(Operator调用关系树)
* 使用上述子类实现Executor的exec接口函数

## Core Features :

* AIMDB是一个内存数据库系统
    * 目前系统版本是0.8
    * 建议运行环境ubuntu-64bits
* AIMDB支持数据存储和索引功能
    * 数据存储空间由AIMDB内部管理
    * 支持hash&Btree索引功能
    * 支持数据类型INT[8,16,32,64],FLOAT[32,64], DATE,TIME,DATETIME
    * 不支持“键”, (主键和外键)
* 主要模块包括
    * 数据库框架、存储管理、索引管理、查询执行模块


## DB Framework :

* **Schema** : 定义Database,Table,Column,Index的上层接口; 保持统一的接口,便于扩展实现其底层结构
    * Table接口(rowtable)
        * bool select (int64_t record_rank, char *dest);
        * bool select (char *row_pointer, char *dest)
        * bool selectCol (int64_t record_rank, int64_t
        column_rank, char *dest)
        * bool selectCol (char *row_pointer, int64_t
        column_rank, char *dest)
        * bool selectCols (int64_t record_rank, int64_t
        column_total, int64_t *column_ranks, char *dest)
        * bool selectCols (char *row_pointer, int64_t
        column_total, int64_t *column_ranks, char *dest)
    * Index接口(hash&bptree)
        * bool set_ls (void *i_data1, void *i_data2, void *info)
        * bool set_ls (void *i_data1[], void *i_data2[], void *info)
        * bool lookup (void *i_data, void *&result)
        * bool lookup (void *i_data[], void *&result)
        * bool lookup (void *i_data, void *info, void *&result)
        * bool lookup (void *i_data[], void *info, void *&result);
        * bool scan_1 (void *i_left, void *info)
        * bool scan_1 (void *i_left[], void *info)
        * bool scan_2 (void *i_right, void *info, void *&result)
        * bool scan_2 (void *i_right[], void *info, void *&result)
* **Catalog** : 
    * Catalog内部函数
        * int64_t registerObj(Object * obj)
        * int64_t obtainId(void)
    * Catalog接口函数
        * bool createDatabase(const char *name, int64_t & d_id);
        * bool createTable(const char *name, TableType type, int64_t & t_id);
        * bool createColumn(const char *name, ColumnType type,int64_t option_size, int64_t & c_id);
        * bool createIndex(const char *name, IndexType type, Key i_key,int64_t & i_id);
        * bool initDatabase(int64_t d_id);
        * Object *getObjById(int64_t o_id);
        * Object *getObjByName(char *o_name);
* **Hash** :
    * hash interface :
        * bool set_ls(void *i_data1, void *i_data2, void *info);
        * bool set_ls(void *i_data1[], void *i_data2[], void *info);
        * bool lookup(void *i_data, void *info, void *&result);
        * bool lookup(void *i_data[], void *info, void *&result);
        * bool insert(void *i_data, void *p_in);
        * bool insert(void *i_data[], void *p_in);
        * bool del(void *i_data);
        * bool del(void *i_data[]);
    ![cpu-1](/resources/db-1.png)
* **Memory Management** :
    * memory management interface :
        * int64_t alloc(char *&p, int64_t size);
        * int64_t free(char *p, int64_t size);
        * int print(void);
    ![cpu-1](/resources/db-2.png)

## Input & Output :

* 输入数据介绍
    * schema文本文件(.txt)和table数据文件(.tab)
    * 分析请求以SelectQuery类给出,main函数调用
* 输出要求说明
    * 运行结果以ResultTable表示,然后将其输出到标准输出 stdout,代码中提供了一个参考输出函数。
    * 请将结果输出标准输出stdout,每行代表一条记录, 一行记录内以一个TAB分隔,且行尾没有TAB,只有换行符。
    * 对于最终提交的结果,标准输出不要有任何其他输出。
    * Tips: 调试你可使用”-v”参数在标准输出,验证输出结果,检查时不会使用”-v”参数。

