/**
 * @file    runaimdb.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * the main entrance of AIMDB 
 *
 */
#include "global.h"
#include "executor.h"
#include <stdio.h>
#include <stdlib.h>
#include <vector>

const char *table_name[] = {
    "part",
    "supplier",
    "partsupp",
    "customer",
    "nation",
    "lineitem",
    "region",
    "orders"
};

int print_flag = false;
int load_schema(const char *filename);
int load_data(const char *tablename[],const char *data_dir, int number);
int test(void);
int testOne (int which);

int main(int argc, char *argv[])
{
    if (argc < 3) {
        printf ("usage- ./runaimdb schema_file data_dir [-v]\n");
        printf ("note-  option -v: print more infomation\n");
        return 0;
    }
    if (argc == 4 && !strcmp(argv[3], "-v")) {
        print_flag = true;
    }
    if (global_init()) {
        printf ("[runaimdb][ERROR][main]: global init error!\n");
        return -1;
    }
    if (load_schema(argv[1])) {
        printf ("[runaimdb][ERROR][main]: load schema error!\n");
        return -2;
    }
    if (load_data(table_name,argv[2],8)) {
        printf ("[runaimdb][ERROR][main]: load data error!\n");
        return -3;
    }
    if (print_flag)
        printf ("start test!\n");

    // here start to test your code by call executor
    test();

    global_shut();
    if (print_flag)
        printf ("finish all test!\n");

    return 0;
}

//----------------------------------------------------------------------------
/**
 * load a database schema from a txt file.
 * @param filename name of schema file, the schema must meet the folowing condition
 * @retval 0  success
 * #retval <0 faliure 
 *
 * schema format 
 * (1) split by one '\t', a row ends with '\n'
 * (2) claim Database,Table,column,index in order
 * (3) no empty row
**/
int load_schema(const char *filename)
{
    int64_t cur_db_id = -1;
    int64_t cur_tb_id;
    int64_t cur_col_id;
    int64_t cur_ix_id;
    Database *cur_db_ptr = NULL;
    Table *cur_tb_ptr = NULL;
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("[load_schema][ERROR]: filename error!\n");
        return -1;
    }
    char buffer[1024];
    while (fgets(buffer, 1024, fp)) {
        int pos = 0, num = 0;
        char *row[16];
        char *ptr = NULL;
        while (buffer[pos] == '\t')
            pos++;
        ptr = buffer + pos;
        while (num < 16) {
            while (buffer[pos] != '\t' && buffer[pos] != '\n')
                pos++;
            row[num++] = ptr;
            ptr = buffer + pos + 1;
            if (buffer[pos] == '\n') {
                buffer[pos] = '\0';
                break;
            }
            buffer[pos] = '\0';
            pos++;
        }
        /*
           //  debug
           for (int ii=0; ii< num; ii++)
           printf ("%s\t", row[ii]);
           printf ("\n");
         */
        if (num >= 16) {
            printf("[load_schema][ERROR]: row with too many field!\n");
            return -2;
        }
        if (!strcmp(row[0], "DATABASE")) {
            if (cur_db_id != -1)
                g_catalog.initDatabase(cur_db_id);
            g_catalog.createDatabase((const char *) row[1], cur_db_id);
            cur_db_ptr = (Database *) g_catalog.getObjById(cur_db_id);
        } else if (!strcmp(row[0], "TABLE")) {
            TableType type;
            if (!strcmp(row[2], "ROWTABLE"))
                type = ROWTABLE;
            else if (!strcmp(row[2], "COLTABLE"))
                type = COLTABLE;
            else {
                printf("[load_schema][ERROR]: table type error!\n");
                return -4;
            }
            g_catalog.createTable((const char *) row[1], type, cur_tb_id);
            cur_tb_ptr = (Table *) g_catalog.getObjById(cur_tb_id);
            cur_db_ptr->addTable(cur_tb_id);
        } else if (!strcmp(row[0], "COLUMN")) {
            ColumnType type;
            int64_t len = 0;
            if (!strcmp(row[2], "INT8"))
                type = INT8;
            else if (!strcmp(row[2], "INT16"))
                type = INT16;
            else if (!strcmp(row[2], "INT32"))
                type = INT32;
            else if (!strcmp(row[2], "INT64"))
                type = INT64;
            else if (!strcmp(row[2], "FLOAT32"))
                type = FLOAT32;
            else if (!strcmp(row[2], "FLOAT64"))
                type = FLOAT64;
            else if (!strcmp(row[2], "DATE"))
                type = DATE;
            else if (!strcmp(row[2], "TIME"))
                type = TIME;
            else if (!strcmp(row[2], "DATETIME"))
                type = DATETIME;
            else if (!strcmp(row[2], "CHARN")) {
                type = CHARN;
                len = atoi(row[3]);
            } else {
                printf("[load_schema][ERROR]: column type error!\n");
                printf("error: %s\n", row[1]);
                return -5;
            }
            g_catalog.createColumn((const char *) row[1], type, len,
                                   cur_col_id);
            cur_tb_ptr->addColumn(cur_col_id);
        } else if (!strcmp(row[0], "INDEX")) {
            IndexType type = INVID_I;
            std::vector < int64_t > cols;
            if (!strcmp(row[2], "HASHINDEX"))
                type = HASHINDEX;
            else if (!strcmp(row[2], "BPTREEINDEX"))
                type = BPTREEINDEX;
            else if (!strcmp(row[2], "ARTTREEINDEX"))
                type = ARTTREEINDEX;
            for (int ii = 3; ii < num; ii++) {
                int64_t oid = g_catalog.getObjByName(row[ii])->getOid();
                cols.push_back(oid);
            }
            Key key;
            key.set(cols);
            g_catalog.createIndex(row[1], type, key, cur_ix_id);
            cur_tb_ptr->addIndex(cur_ix_id);
        } else {
            printf("[load_schema][ERROR]: o_type error!\n");
            return -3;
        }
    }
    g_catalog.initDatabase(cur_db_id);
    if (print_flag)
        g_catalog.print();
    return 0;
}

/**
 * load table data from txt files
 * @param tablename names of tables
 * @param data_dir the dir of data files corresponding to the table, end with '/'
 * @param number number of tables to load data
 * @retval 0  success
 * @retval <0 faliure
 *
 * naming rule of data files
 * 
 * each data file should be named "table_name.tab" and meet the following requirement
 *
 * data format
 *
 * (1) split by one '\t', a row ends with '\n'
 * (2) no empty row
 *
**/
int load_data(const char *tablename[],const char *data_dir, int number)
{
    for (int ii = 0; ii < number; ii++) {
        char filename[1024];
        strcpy (filename, data_dir);
        strcat (filename, tablename[ii]);
        strcat (filename, ".tab");
        FILE *fp = fopen(filename, "r");
        if (fp == NULL) {
            printf("[load_data][ERROR]: filename error!\n");
            return -1;
        }
        Table *tp =
            (Table *) g_catalog.getObjByName((char *) tablename[ii]);
        if (tp == NULL) {
            printf("[load_data][ERROR]: tablename error!\n");
            return -2;
        }
        int colnum = tp->getColumns().size();
        BasicType *dtype[colnum];
        for (int ii = 0; ii < colnum; ii++)
            dtype[ii] =
                ((Column *) g_catalog.getObjById(tp->getColumns()[ii]))->
                getDataType();
        int indexnum = tp->getIndexs().size();
        Index *index[indexnum];
        for (int ii = 0; ii < indexnum; ii++)
            index[ii] =
                (Index *) g_catalog.getObjById(tp->getIndexs()[ii]);

        char buffer[2048];
        char *columns[colnum];
        void *columnsvoid[colnum];
        char data[colnum][1024];
        while (fgets(buffer, 2048, fp)) {
            // insert table
            columns[0] = buffer;
            int64_t pos = 0;
            for (int64_t ii = 1; ii < colnum; ii++) {
                for (int64_t jj = 0; jj < 2048; jj++) {
                    if (buffer[pos] == '\t') {
                        buffer[pos] = '\0';
                        columns[ii] = buffer + pos + 1;
                        pos++;
                        break;
                    } else
                        pos++;
                }
            }
            /*
               // debug
               for (int ii= 0; ii< colnum; ii++) {
               printf ("%s\t", columns[ii]);
               }
               printf ("\n");
             */
            while (buffer[pos] != '\n')
                pos++;
            buffer[pos] = '\0';
            for (int64_t ii = 0; ii < colnum; ii++) {
                BasicType *p = dtype[ii];
                p->formatBin(data[ii], columns[ii]);
                columns[ii] = data[ii];
            }
            tp->insert(columns);
            void *ptr = tp->getRecordPtr(tp->getRecordNum() - 1);
            // insert index
            for (int ii = 0; ii < indexnum; ii++) {
                int indexsz = index[ii]->getIKey().getKey().size();
                for (int jj = 0; jj < indexsz; jj++)
                    columnsvoid[jj] =
                        data[tp->getColumnRank
                             (index[ii]->getIKey().getKey()[jj])];
                index[ii]->insert(columnsvoid, ptr);
            }
        }
        if (print_flag)
            tp->printData();
    }
    return 0;
}

///--------------------------------tpch test-----------------------------------------
SelectQuery querys[22] = {
    /** TQ-1 select 1 列，1个filter, 小数据集
    select s_name
    from supplier
    where s_nationkey = 18
    **/
    {
        1,
        1,
        {
            {"s_name",NONE_AM}
        },
        1,
        {
            "supplier"
        },
        {
            1,
            {
                { {"s_nationkey",NONE_AM},EQ,"18" }
            }
        },
        0,{},{},0,{}
    },
    /** TQ-2 select 2 列， 2个filter, 小数据集，其中一个filter列上有index
             (index: ps_suppkey)
    select ps_partkey,ps_availqty
    from partsupp
    where ps_suppkey = 6 and ps_availqty < 8000
    **/
    {
        1,
        2,
        {
            {"ps_partkey",NONE_AM},
            {"ps_availqty",NONE_AM}
        },
        1,
        {
            "partsupp"
        },
        {
            2,
            {
                { {"ps_suppkey",NONE_AM},EQ,"6" },
                { {"ps_availqty",NONE_AM},LT,"8000" }
            }
        },
        0,{},{},0,{}
    },
    /** TQ-3
    **/
    {0},
    /** TQ-4
    **/
    {0},
    /** TQ-5
    **/
    {0},
    /** TQ-6 2个表join, 没有filter条件，没有index, 输出2列（每个表各选1列），小数据集
    select c_name,o_totalprice
    from customer,orders
    where c_custkey = o_custkey
    **/
    {
        1,
        2,
        {
            {"c_name",NONE_AM},
            {"o_totalprice",NONE_AM}
        },
        2,
        {
            "customer",
            "orders"
        },
        {
            1,
            {
                { {"c_custkey",NONE_AM},LINK,"o_custkey" }
            }
        },
        0,{},{},0,{}
    },
    /** TQ-7 2个表join, 没有filter条件，有index, 输出2列（每个表各选1列），小数据集
             (index: r_regionkey)
    select c_name,o_totalprice
    from customer,orders
    where c_custkey = o_custkey
    **/
    {
        1,
        2,
        {
            {"n_name",NONE_AM},
            {"r_name",NONE_AM}
        },
        2,
        {
            "nation",
            "region"
        },
        {
            1,
            {
                { {"n_regionkey",NONE_AM},LINK,"r_regionkey" }
            }
        },
        0,{},{},0,{}
    },
    /** TQ-8
    **/
    {0},
    /** TQ-9
    **/
    {0},
    /** TQ-10
    **/
    {0},
    /** TQ-11 2个表join, 各有1个filter条件，没有index, 输出2列（每个表各选1列），小数据集
    select c_name,o_totalprice
    from customer,orders
    where c_custkey = o_.custkey and c_nationkey = 18 and totalprice >20000
    **/
    {
        1,
        2,
        {
            {"c_name",NONE_AM},
            {"o_totalprice",NONE_AM}
        },
        2,
        {
            "customer",
            "orders"
        },
        {
            3,
            {
                { {"c_custkey",NONE_AM},LINK,"o_custkey" },
                { {"c_nationkey",NONE_AM},EQ,"18" },
                { {"o_totalprice",NONE_AM},GT,"20000" }
            }
        },
        0,{},{},0,{}    
    },
    /** TQ-12
    **/
    {0},
    /** TQ-13
    **/
    {0},
    /** TQ-14
    **/
    {0},
    /** TQ-15
    **/
    {0},
    /** TQ-16 TQ2基础上，加一个group by key, 一个aggregation，小数据集
    select ps_partkey,sum(ps_supplycost)
    from partsupp
    where ps_suppkey = 6 and ps_supplycost < 2000
    group by ps_partkey
    **/
    {
        1,
        2,
        {
            {"ps_partkey",NONE_AM},
            {"ps_availqty",SUM}
        },
        1,
        {
            "partsupp"
        },
        {
            2,
            {
                { {"ps_suppkey",NONE_AM},EQ,"6" },
                { {"ps_supplycost",SUM},LT,"2000" }
            }
        },
        1,
        {
            {"ps_partkey",NONE_AM}
        },
        {0},0,{}
    },
    /** TQ-17
    **/
    {0},
    /** TQ-18 TQ6基础上，加一个group by key, 2个aggregation，小数据集
    select c_name,SUM(totalprice),MAX(totalprice)
    from customer,orders
    where c_custkey = o_custkey
    group by c_name
    **/
    {
        1,
        3,
        {
            {"c_name",NONE_AM},
            {"o_totalprice",SUM},
            {"o_totalprice",MAX}
        },
        2,
        {
            "customer",
            "orders"
        },
        {
            1,
            {
                { {"c_custkey",NONE_AM},LINK,"o_custkey" }
            }
        },
        1,
        {
            {"c_name",NONE_AM}
        },
        {0},0,{}
    },
    /** TQ-19
    **/
    {0},
    /** TQ-20
    **/
    {0},
    /** TQ-21 TQ2基础上，按1个输出列进行排序，小数据集
    (index: ps_suppkey)
    select ps_partkey,ps_availqty
    from partsupp
    where ps_suppkey = 6 and ps_availqty < 8000
    order by ps_availqty 
    **/
    {
        1,
        2,
        {
            {"ps_partkey",NONE_AM},
            {"ps_availqty",NONE_AM}
        },
        1,
        {
            "partsupp"
        },
        {
            2,
            {
                { {"ps_suppkey",NONE_AM},EQ,"6" },
                { {"ps_availqty",NONE_AM},LT,"8000" }
            }
        },
        0,{},{},
        1,
        {
            {"ps_availqty",NONE_AM}
        }
    },
    /** TQ-22
    **/
    {0}
};

int test (void) {
    const int tq_num[8] = {1,2,6,7,11,16,18,21};
    for (int ii=0; ii< 8; ii++) {
        testOne (tq_num[ii]);
    }
    return 0;
}

int testOne (int which)
{
    // here we run some test with random data
    char file_name[1024];
    sprintf (file_name,"../Lab3_Test1/result/aimdb_result/TQ%d.tab",which);
    FILE *fp = fopen (file_name,"w");
    if (fp == NULL) {
        if (print_flag) {
            printf ("in testOne: file_name error!\n");
            return 0;
        }
    }
    Executor executor;
/*   
    //  example query:
    //  select example_column_3,example_column_5,example_column_14
    //  from example_table_1,exampel_table_2
    //  where example_column_4 == example_column_12
    SelectQuery query = { 1,  // database_id
                          3,  // select_number
                          {   // select_column
                            { "example_column_3", NONE_AM  },
                            { "example_column_5", NONE_AM  },
                            { "example_column_14",NONE_AM  },
                            {}   
                          },
                          2,  // from_number
                          {   // from_table
                              "example_table_1","example_table_2","",""
                          },
                          {     // where
                            1,
                            {   //conditions 
                              { {"example_column_4",NONE_AM},LINK,"example_column_12"},
                              {},
                              {},
                              {}
                            }
                          },
                          0,  // groupby_number
                          {}, // greoup_by
                          {}, // having
                          0,  // orderby_number
                          {}  // orderby
                        };
*/
    if (querys[which-1].database_id == 0) {
        printf ("current query not provided! and query should range in 1-21!\n");
        return -1;
    }
    ResultTable result = {0};
    int stat = executor.exec(&querys[which-1], &result);
    while (stat > 0) {
        result.print ();
        result.dump  (fp);
        stat = executor.exec(NULL, &result);
    }
    fprintf (fp,"\n");
    fclose (fp);
    result.shut ();
    return 0;
}
