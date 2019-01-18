/**
 * @file    executor.h
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */

#ifndef _EXECUTOR_H
#define _EXECUTOR_H

#include "catalog.h"
#include "mymemory.h"

uint32_t gethash(char *key, BasicType * type);

/** aggrerate method. */
enum AggrerateMethod {
    NONE_AM = 0, /**< none */
    COUNT,       /**< count of rows */
    SUM,         /**< sum of data */
    AVG,         /**< average of data */
    MAX,         /**< maximum of data */
    MIN,         /**< minimum of data */
    MAX_AM
};

/** compare method. */
enum CompareMethod {
    NONE_CM = 0,
    LT,        /**< less than */
    LE,        /**< less than or equal to */
    EQ,        /**< equal to */
    NE,        /**< not equal than */
    GT,        /**< greater than */
    GE,        /**< greater than or equal to */
    LINK,      /**< join */
    MAX_CM
};

/** definition of request column. */
struct RequestColumn {
    char name[128];    /**< name of column */
    AggrerateMethod aggrerate_method;  /** aggrerate method, could be NONE_AM  */
};

/** definition of request table. */
struct RequestTable {
    char name[128];    /** name of table */
};

/** definition of compare condition. */
struct Condition {
    RequestColumn column;   /**< which column */
    CompareMethod compare;  /**< which method */
    char value[128];        /**< the value to compare with, if compare==LINK,value is another column's name; else it's the column's value*/
};

/** definition of conditions. */
struct Conditions {
    int condition_num;      /**< number of condition in use */
    Condition condition[4]; /**< support maximum 4 & conditions */
};

/** definition of selectquery.  */
class SelectQuery {
  public:
    int64_t database_id;           /**< database to execute */
    int select_number;             /**< number of column to select */
    RequestColumn select_column[4];/**< columns to select, maximum 4 */
    int from_number;               /**< number of tables to select from */
    RequestTable from_table[4];    /**< tables to select from, maximum 4 */
    Conditions where;              /**< where meets conditions, maximum 4 & conditions */
    int groupby_number;            /**< number of columns to groupby */
    RequestColumn groupby[4];      /**< columns to groupby */
    Conditions having;             /**< groupby conditions */
    int orderby_number;            /**< number of columns to orderby */
    RequestColumn orderby[4];      /**< columns to orderby */
};  // class SelectQuery

/** definition of result table.  */
class ResultTable {
  public:
    int column_number;       /**< columns number that a result row consist of */
    BasicType **column_type; /**< each column data type */
    char *buffer;         /**< pointer of buffer alloced from g_memory */
    int64_t buffer_size;  /**< size of buffer, power of 2 */
    int row_length;       /**< length per result row */
    int row_number;       /**< current usage of rows CURRENT NUMBER OF ROW*/
    int row_capicity;     /**< maximum capicity of rows according to buffer size and length of row  MAXIMUN OF ROW */
    int *offset;
    int offset_size;

    /**
     * init alloc memory and set initial value
     * @col_types array of column type pointers
     * @col_num   number of columns in this ResultTable
     * @param  capicity buffer_size, power of 2
     * @retval >0  success
     * @retval <=0  failure
     */
    int init(BasicType *col_types[],int col_num,int64_t capicity = 1024);
    /**
     * calculate the char pointer of data spcified by row and column id
     * you should set up column_type,then call init function
     * @param row    row id in result table
     * @param column column id in result table
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    char* get_RC(int row, int column);
    /**
     * write data to position row,column
     * @param row    row id in result table
     * @param column column id in result table
     * @data data pointer of a column
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    int write_RC(int row, int column, void *data);
    /**
     * print result table, split by '\t', output a line per row 
     * @retval the number of rows printed
     */
    int print(void);
    /**
     * write to file with FILE *fp
     */
    int dump(FILE *fp);
    /**
     * free memory of this result table to g_memory
     */
    int shut(void);
};  // class ResultTable


/** definition of Operator.  */
class Operator {
    protected:
        RowTable *table_out; 	        /**< This is only for RPattern and getColumnRank */
        					            /**< We only need its skeleton: cols_name[], cols_id[] and col_num. */
        					            /**< record data inside is meaningless. */
        RowTable *table_in[4];          /**< tables input in each operation. */
        int64_t table_out_col_num = 0;	/**< the number of column in table_out. */
        ResultTable result;             /**< each operator got its own ResultTable(Buffer) except Scan Operator. */
        BasicType   **in_col_type;    	/**< column types of input tables. */
	public:
        int64_t Ope_id = 0;	        
        /**
         * construction of Operator.
         */
        Operator(void) {}
        /**
         *  operator init
         *  @retval false for failure 
         *  @retval true  for success 
         */
		virtual bool	init    () = 0;	
        /**
         * get next record and put it in resulttable 
         * @param result buffer to store the record 
         * @retval false for failure 
         * @retval true  for success 
         */
		virtual	bool	get_Next (ResultTable *result) = 0;
        /**
         * where this operator is end 
         * @retval false not end
         * @retval true  run end
         */
		virtual	bool	is_End   () = 0;
        /**
         * close the operator and release memory
         * @retval false for failure 
         * @retval true  for success 
         */
        virtual bool    close   () = 0;
        /**
         * get the output result table of this Operator
         * @retval table_out 
         */
        RowTable *getTableOut   () { 
            return table_out; 
        }


};

/** definition of Scan operator. */
class Scan : public Operator {
    private:
        int64_t current_row;/**< row number has been scaned. */
        int64_t col_num;    /**< number of columns           */
	public:
        /**
         * Scan rowtable from table_in
         * @param tablename the table_in that this function will scan 
         */
        Scan(char *tablename);
        /**
         * init scan operator 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool	init	();
        /**
         * get next record of scan 
         * @param result the buffer to store result of scan 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    get_Next (ResultTable *result);
        /**
         * judge where scan is end 
         * @retval false not end
         * @retval true  run end
         */
        bool    is_End   ();
        /**
         * close scan and release memory
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    close   ();
        /**
         * write a row in resulttable
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    writeRow (char* buffer, ResultTable *result){
            for (int current_col = 0; current_col < col_num; current_col++) {
                if (!this->table_in[0]->selectCol(current_row, current_col, buffer)) {
                    g_memory.free(buffer, 128);
                    return false;
                }
                if (!result->write_RC(0, current_col, buffer)) {
                    g_memory.free(buffer, 128);
                    return false;
                }
            }
            return true;
        } 

        bool    equalornot(){
            if(this->current_row == this->table_in[0]->getRecordNum()){
                return true;
            }else{
                return false;
            }
        }
};

/** definition of filter operaotr*/
class Filter : public Operator {
    private:
        Operator *prior_op;             /**< operator of prior operation.     */
        int64_t col_rank;               /**< which column need to be filtered.*/
        char value[128];                /**< const value                      */ 
        BasicType *value_type;          /**< column types of filter columns   */
        CompareMethod compare_method;   /**< compare methods                  */
    public:
        /**
         * construction of filter Operator
         * @param Op prior_op that needs to inherit
         * @param condi the filter conditions.
         */
        Filter(Operator *Op, Condition *condi);
        /**
         * init operator 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    init    (); 
        /**
         * get next record 
         * @param result the buffer to store result. 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    get_Next (ResultTable *result);
        /**
         * judge whether is end
         * @retval false not end
         * @retval true  run end
         */
        bool    is_End   ();
        /**
         * close operator and release memory
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    close   ();
        /**
         * init filter resulttable
         * @retval false for failure 
         * @retval true  for success 
         */
        void    resinit (){
            RPattern in_RP = this->table_in[0]->getRPattern();
            int in_colnum = this->table_in[0]->getColumns().size();
            in_col_type = new BasicType *[in_colnum];
            for(int i=0; i < in_colnum; i++)
                in_col_type[i] = in_RP.getColumnType(i);
            this->result.init(in_col_type, in_colnum);
        }
        /**
         * check the result of cmp
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    compare_exec (char* cmpSrcA_ptr, char* cmpSrcB_ptr){
            if (this->compare_method == LT)
                return this->value_type->cmpLT(cmpSrcA_ptr, cmpSrcB_ptr);
            else if (this->compare_method == LE)
                return this->value_type->cmpLE(cmpSrcA_ptr, cmpSrcB_ptr);
            else if (this->compare_method == EQ)
                return this->value_type->cmpEQ(cmpSrcA_ptr, cmpSrcB_ptr);
            else if (this->compare_method == NE)
                return !this->value_type->cmpEQ(cmpSrcA_ptr, cmpSrcB_ptr);
            else if (this->compare_method == GT)
                return this->value_type->cmpGT(cmpSrcA_ptr, cmpSrcB_ptr);
            else if (this->compare_method == GE)
                return this->value_type->cmpGE(cmpSrcA_ptr, cmpSrcB_ptr);
            else return false;            
        }
        /**
         * copy result
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    resCopy (bool flag, ResultTable *result){
            if (flag) {
                char* buffer;
                for (int j = 0; j < (int)this->table_out->getColumns().size(); j++) {
                    buffer = this->result.get_RC(0L, j);
                    if (!result->write_RC(0, j, buffer)) {
                        return false;
                    }
                }
            }   
            return true;         
        }
        /**
         * shut memory
         * @retval false for failure 
         * @retval true  for success 
         */
        void    MemShut  (){
            delete prior_op;
            result.shut();
            delete[] in_col_type;
        }
}; 

/** definition of hashjoin. */
class HashJoin : public Operator {
    private:
        Operator *Op[2] = {NULL, NULL};    /**< prior operator from tow join table. */
        int operator_num = 0;              /**< number of join table                */
        int cond_num = 0;                  /**< number of join conditions           */ 
        int col_B_row = 0;                 /**< the rownumber of table  B           */
        int col_num[2] = {0, 0};           /**< the clolumn numbers of two table    */
        int64_t col_A_oid = -1;            /**< column A object  id                 */
        int64_t col_B_oid = -1;            /**< column B object  id                 */
        int64_t col_A_rank = -1;           /**< column A rank                       */
        int64_t col_B_rank = -1;           /**< column B rank                       */
        ResultTable row_i[200000];         /**< result table to store temp result   */
        BasicType * value_type;            /**< result type of result               */
        HashIndex * hash_index = NULL;     /**< hash index of hash table            */
        HashTable * hash_table = NULL;     /**< hash tale to store data             */
        BasicType** col_B_type;            /**< column types of table B             */
    public:
        /**
         * construction of HashJoin
         * @param operator_num tables need to Join
         * @param Op prior oprators
         * @param cond_num condition number of join 
         * @param join condtions
         */
        HashJoin(int operator_num, Operator **Op, int cond_num, Condition *condi);
        /**
         * init hashjoin.
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    init    ();
        /**
         * get next record of hash Join
         * @param result buffer to store result 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    get_Next (ResultTable *result);
        /**
         * judge whether is end 
         * @retval false not end
         * @retval true  run end
         */
        bool    is_End   ();
        /**
         * close operator and release memory.
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    close   (); 
        /**
         * first deal with the data
         * @retval false for failure 
         * @retval true  for success 
         */
        void    Fdeal   (int operator_num, Operator **Op){
            this->operator_num = operator_num;
            if (Op[1]->getTableOut()->getRecordNum()>Op[0]->getTableOut()->getRecordNum()) {
                Operator* op_switch = Op[1];
                Op[1] = Op[0];
                Op[0] = op_switch;
            }
            for (int i=0; i < operator_num; i++) {
                this->Op[i] = Op[i];
                table_in[i] = Op[i]->getTableOut();
                col_num[i] = (int)table_in[i]->getColumns().size();
                table_out_col_num += col_num[i];
            }
        }
        /**
         * make sure colA belongs to table_0
         * @retval false for failure 
         * @retval true  for success 
         */
        void    MakeOrder (){
            bool flag = true;
            auto & cols_id = table_in[0]->getColumns();
            for (int i = 0; i < col_num[0]; i++) 
                if (cols_id[i]==col_A_oid) {
                    flag = false;
                    break;
                }
            if (flag) {
                int64_t col_switch_tmp = col_A_oid;
                col_A_oid = col_B_oid;
                col_B_oid = col_switch_tmp;
            }
            col_A_rank = table_in[0]->getColumnRank(col_A_oid);
            col_B_rank = table_in[1]->getColumnRank(col_B_oid);            
        }
        /**
         * allocate value
         * @retval false for failure 
         * @retval true  for success 
         */
        void    alloValue  (char * value, char * format_value){
            while (!Op[1]->is_End()) {
                row_i[col_B_row].init(col_B_type, col_num[1]);
                Op[1]->get_Next(&row_i[col_B_row]);
                
                value = row_i[col_B_row].get_RC(0, col_B_rank);
                BasicType * this_type = table_in[1]->getRPattern().getColumnType(col_B_rank);

                this_type->formatTxt(format_value, value);
                uint32_t hash_value = gethash(format_value, this_type);
                hash_table->add(hash_value, (char *)&row_i[col_B_row]);
                
                col_B_row++ ;
            }
            this->value_type = table_in[0]->getRPattern().getColumnType(col_A_rank);
        }
        /**
         * write row
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    WriteRow  (bool flag, ResultTable *result){
            if (flag) {
                char* buffer;
                for (int j = 0; j < col_num[0]; j++) {
                    buffer = this->result.get_RC(0L, j);
                    if (!result->write_RC(0, j, buffer))
                    {
                        return false;
                    } 
                }
                for (int j = 0; j < col_num[1]; j++) {
                    buffer = row_i->get_RC(0L, j);
                    if (!result->write_RC(0, col_num[0] + j, buffer))
                    {
                        return false;
                    } 
                }
            }
            return true;
        }
        /**
         * shut memory
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    ShutMem   (){
            delete hash_table;
            result.shut();
            delete []in_col_type;
            for (int i = 0; i < col_B_row; i++) {
                row_i[i].shut();
            }
            for (int i = 0; i < operator_num; i++) 
                if (!Op[i]->close()) return false;
            for (int i = 0; i < operator_num; i++) 
                delete Op[i];
            delete [] col_B_type;
            return true;
        }
};

/** definition of project   */
class Project : public Operator {
    private:
        Operator *prior_op;     /**< prior operators                      */
        int64_t col_tot;        /**< number of columns being projected.   */
        int64_t col_rank[4];    /**< rank sets of columns projected       */
        RPattern in_RP;         /**< inside class use                     */
    public:
        /**
         * @brief construction of project 
         * @param Op prior operators 
         * @param col_tot total columns projected 
         * @param cols_name columns name of projected column 
        */
        Project(Operator *Op, int64_t col_tot, RequestColumn *cols_name);
        /** 
         * @brief init of project 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    init    ();
        /**
         * @brief get next record of operator 
         * @param result buffer to store result 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    get_Next (ResultTable *result);
        /**
         * @brief judge whether is end
         * @retval false not end
         * @retval true  run end
         */
        bool    is_End   (); 
        /**
         * close operator and release memory.
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    close   ();
        /**
         * @brief init result table
         * @retval false for failure 
         * @retval true  for success 
         */
        void    ResInit  (int in_colnum){
            in_col_type = new BasicType *[in_colnum];
            for(int i=0; i < in_colnum; i++)
                in_col_type[i] = this->in_RP.getColumnType(i);
            this->result.init(in_col_type, in_colnum);
        }
        /**
         * @brief Write a row
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    WriteRow (ResultTable *result){
            char* buffer ;
            if (!this->prior_op->get_Next(&this->result)){
                return false;
            }
            for (int i = 0; i < col_tot; i++) { 
                buffer = this->result.get_RC(0L, col_rank[i]);
                if (!result->write_RC(0, i, buffer)) {
                    return false;
                }
            }
            return true;            
        }
        /**
         * @brief shut memory
         * @retval false for failure 
         * @retval true  for success 
         */
        void    ShutMem  (){
            delete prior_op;
            result.shut();
            delete []in_col_type;
        }
};


/** definiton of OrderBy   */
class OrderBy :public Operator {
    private:
        Operator *prior_op;              /**< prior Operator                       */ 
        int64_t index = 0;               /**< index that has been ordered          */
        int64_t col_num = 0;             /**< number of columns                    */
        int64_t record_size;             /**< size of each record                  */
        int64_t row_length;              /**< length of each row                   */
        BasicType *compare_col_type[4];  /**< column types of compare conditons    */
        int64_t compare_col_rank[4];     /**< ranks of each compare condtions      */
        int64_t compare_col_offset[4];   /**< offset of each compared columns      */
        int64_t order_by_num;            /**< number of order conditions           */
        ResultTable re;                  /**< temp result table to store result    */
        RPattern rpattern;               /**< inside class use                     */
        RequestColumn *in_cols_name;     /**< inside class use                     */
        int64_t low;                     /**< inside class use                     */
        int64_t high;                    /**< inside class use                     */
        char *key;                       /**< inside class use                     */
    public:
        /**
         * @brief construction of OrderBy
         * @param Op prior operators 
         * @param order_by_num number of condtions in order 
         * @param cols_name names of columns 
         */
        OrderBy(Operator * op_ptr, int64_t order_by_num, RequestColumn *cols_name);
        /**
         * @brief quick sort 
         * @param buffer the begin of orderby address
         * @param left left (low)side of order sequence
         * @param right right (high)side of order sequence 
         */
        void quick_sort(char *buffer ,int64_t left, int64_t right);
        /**
         * @brief compare tow buffers 
         * @param l first buffer
         * @param r second buffer 
         * @retval 2 for >
         * @retval 1 for =
         * @retval 0 for <
         */
        int cmp(void*l,void*r);
        /** 
         * @brief init of project 
         */
        bool    init();
        /**
         * @brief get next record of operator 
         * @param result buffer to store result 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    get_Next(ResultTable *result);
        /**
         * @brief judge whether is end
         * @retval false not end
         * @retval true  run end
         */
        bool    is_End() ;
        /**
         * @brief close operator and release memory.
         * @retval false for failure 
         * @retval true  for success 
         */
        bool    close() ;
    private:
        /**
         * @brief get pointer to prior Operator
         * @retval pointer to prior Operator
         */
        Operator *get_prior_operator(){
            return this->prior_op;
        }
        /**
         * @brief get index that has been ordered
         * @retval index that has been ordered
         */
        int64_t get_index(){
            return this->index;
        }
        /**
         * @brief get number of columns
         * @retval number of columns
         */
        int64_t get_number_of_columns(){
            return this->col_num;
        }
        /**
         * @brief get size of each record
         * @retval size of each record
         */
        int64_t get_record_size(){
            return this->record_size;
        }
        /**
         * @brief get length of each row
         * @retval length of each row
         */
        int64_t get_row_length(){
            return this->row_length;
        }
        /**
         * @brief get ptr to column types of compare conditons
         * @retval ptr to column types of compare conditons
         */
        BasicType **get_compare_col_type_ptr(){
            return this->compare_col_type;
        }
        /**
         * @brief get ptr to ranks of each compare condtions
         * @retval ptr to ranks of each compare condtions
         */
        int64_t *get_compare_col_rank(){
            return this->compare_col_rank;
        }
        /**
         * @brief get ptr to offset of each compared columns
         * @retval ptr to offset of each compared columns
         */
        int64_t *get_compare_col_offset(){
            return this->compare_col_offset;
        }
        /**
         * @brief get number of order conditions
         * @retval number of order conditions
         */
        int64_t get_order_by_num(){
            return this->order_by_num;
        }
        /**
         * @brief get temp result table to store result
         * @retval temp result table to store result
         */
        ResultTable get_ResultTable(){
            return this->re;
        }
        /**
         * @brief get rpattern
         * @retval rpattern inside
         */
        RPattern get_rpattern(){
            return this->rpattern;
        }
        /**
         * @brief get prior tableout
         * @retval prior tableout
         */
        RowTable *get_prior_tableout(){
            return this->get_prior_operator()->getTableOut();
        }
        /**
         * @brief get prior column rank
         * @retval prior column rank
         */
        int64_t get_prior_col_rank(int64_t object_oid){
            return this->get_prior_tableout()->getColumnRank(object_oid);
        }
        /**
         * @brief init col type
         */
        void init_col_type(){
            for(int i=0; i < this->col_num; i++){
                this->in_col_type[i] = this->get_rpattern().getColumnType(i);                  
            }
        }
        /**
         * @brief init col
         */
        void init_col(){
            for(int i = 0;i < this->order_by_num;i++)
            {
                Object *col_ptr = g_catalog.getObjByName(this->in_cols_name[i].name);
                // this->compare_col_rank[i] = this->get_prior_operator()->getTableOut()->getColumnRank(col_ptr->getOid());
                // this->compare_col_rank[i] = this->get_prior_tableout()->getColumnRank(col_ptr->getOid());
                this->compare_col_rank[i] = this->get_prior_col_rank(col_ptr->getOid());
                this->compare_col_type[i] = this->get_rpattern().getColumnType(compare_col_rank[i]);
                this->compare_col_offset[i] = this->get_rpattern().getColumnOffset(compare_col_rank[i]);
            }
        }
        /**
         * @brief check not end
         */
        bool check_not_end(){
            return (!this->get_prior_operator()->is_End() && this->get_prior_operator()->get_Next(&this->result));
        }
        /**
         * @brief write result col
         */
        void write_result_col(int row, char *buffer){
            for(int j = 0;j < this->col_num;j++)
            {
                buffer = result.get_RC(0, j);
                re.write_RC(row, j, buffer); 
            }
        }
        /**
         * @brief 
         */
        void qsort_helper(char *buffer){
            while(this->low < this->high)
            {
                while(this->low < this->high && cmp(buffer + this->high * this->row_length, this->key))
                    this->high --;
                memcpy(buffer+this->low*this->row_length, buffer + this->high*this->row_length, this->row_length);

                while(this->low <this->high && cmp(this->key, buffer + this->low*this->row_length) )
                    this->low++;
                memcpy(buffer + this->high*this->row_length , buffer + this->low*this->row_length, this->row_length);

            }
        }
        /**
         * @brief 
         */
        bool check_stop_get_Next(ResultTable *result, char *buffer){
            for(int i = 0;i < this->col_num; i++)
            {
                buffer = this->re.get_RC(this->index, i); 
                if(!result->write_RC(0, i, buffer)){
                return false;
                }
            }    
            return true;
        }
};

/** definition of GroupBy */
class GroupBy : public Operator {
    private:
        Operator *prior_op;                     /**< operator of prior oprators           */
        int64_t row_size;                       /**< size of row                          */
        int aggrerate_num = 0;                  /**< aggrerate number                     */
        int non_aggrerate_num = 0;              /**< number of non aggrerate              */
        BasicType *aggrerate_type[4];           /**< type of each aggrerate               */
        BasicType *non_aggrerate_type[4];       /**< type of each non aggrerate           */
        size_t aggrerate_off[4];                /**< aggrerate offset                     */
        size_t non_aggrerate_off[4];            /**< non aggrerate method offset          */ 
        int aggrerate_index = 0;                /**< index has been group                 */
        int non_aggrerate_index = 0;            /**< index of non aggrerated              */
        AggrerateMethod aggrerate_method[4];    /**< aggrerate methods                    */
        ResultTable tmp_one;                    /**< buffer to store one result           */
        ResultTable tmp_result;                 /**< buffer to store tmp result           */ 
        int col_num = 0;                        /**< number of column                     */
        int index = 0;                          /**< index of current                     */
        RequestColumn *req_col_ptr;             /**< inside class use                     */
        RPattern rpattern;                      /**< inside class use                     */
        int agg_i;                              /**< inside class use                     */
        char* datain;                           /**< inside class use                     */
        char* dataout;                          /**< inside class use                     */
        int64_t *pattern_count_ptr;             /**< inside class use                     */
    public:
        /**
         * @brief construction of OrderBy
         * @param Op prior operators 
         * @param groupby_num number of condtions to groupby 
         * @param req_col names of columns 
         */
        GroupBy(Operator *Op, int groupby_num, RequestColumn req_col[4]);
        /** 
         * @brief init of project 
         */
        bool    init();
        /**
         * @brief get next record of operator 
         * @param result buffer to store result 
         * @retval false for failure 
         * @retval true  for success 
         */
        bool get_Next(ResultTable *result);
        /**
         * @brief judge whether is end
         * @retval false not end
         * @retval true  run end
         */
        bool    is_End() ;
        /**
         * @brief close operator and release memory.
         * @retval false for failure 
         * @retval true  for success 
         */ 
        bool    close();
    private:
        /**
         * @brief get hash from a string with length
         * @param str input a string 
         * @param length the length of string
         * @retval hash number
         */
        int64_t hash(char *str, int64_t length);
        /**
         * @brief do aggregation on a column
         * @param method Aggregation method
         * @param probe_result the result of hashtable::probe
         * @param agg_i the index of aggregation
         * @retval false for failure 
         * @retval true  for success 
         */
        bool aggrerate(AggrerateMethod method, uint64_t probe_result, int agg_i);

        /**
         * @brief init non_aggre
         */
        void init_non_aggre(int64_t col_rank){
            this->non_aggrerate_off[non_aggrerate_index] = col_rank;
            this->non_aggrerate_type[non_aggrerate_index] = table_out->getRPattern().getColumnType(col_rank);
            this->non_aggrerate_num++;
            this->non_aggrerate_index++;
        }
        /**
         * @brief init aggrerate
         */
        void init_aggre(int64_t col_rank){
            this->aggrerate_off[aggrerate_index] = col_rank;
            this->aggrerate_type[aggrerate_index] = table_out->getRPattern().getColumnType(col_rank);
            this->aggrerate_method[aggrerate_index] = this->req_col_ptr[col_rank].aggrerate_method;
            this->aggrerate_num++;
            this->aggrerate_index++;
        }
        /**
         * @brief get pointer to prior Operator
         * @retval pointer to prior Operator
         */
        Operator *get_prior_operator(){
            return this->prior_op;
        }
        /**
         * @brief get prior tableout
         * @retval prior tableout
         */
        RowTable *get_prior_tableout(){
            return this->get_prior_operator()->getTableOut();
        }
        /**
         * @brief get prior column rank
         * @retval prior column rank
         */
        int64_t get_prior_col_rank(int64_t object_oid){
            return this->get_prior_tableout()->getColumnRank(object_oid);
        }
        /**
         * @brief get prior column rank
         * @retval prior column rank
         */
        RPattern get_prior_rpattern(){
            return this->get_prior_tableout()->getRPattern();
        }
        /**
         * @brief get rpattern
         * @retval rpattern inside
         */
        RPattern get_rpattern(){
            return this->rpattern;
        }
        /**
         * @brief sum handler
         * 
         */
        void sum_handler(){

        }
        /**
         * @brief avg handler
         * 
         */
        bool avg_handler(){
            switch (aggrerate_type[this->agg_i]->getTypeCode()){
                case INT8_TC: {
                    int8_t agg_result = *(int8_t *)this->datain + *(int8_t *)this->dataout;
                    memcpy(this->dataout, (void*)&agg_result, 1);
                    break;
                }
                case INT16_TC: {
                    int16_t agg_result = *(int16_t *)this->datain + *(int16_t *)this->dataout;
                    memcpy(this->dataout, (void*)&agg_result, 2);
                    break;
                }
                case INT32_TC: {
                    int32_t agg_result = *(int32_t *)this->datain + *(int32_t *)this->dataout;
                    memcpy(this->dataout, (void*)&agg_result, 4);
                    break;
                }
                case INT64_TC: {
                    int64_t agg_result = *(int64_t *)this->datain + *(int64_t *)this->dataout;
                    memcpy(this->dataout, (void*)&agg_result, 8);
                    break;
                }
                case FLOAT32_TC: {
                    float agg_result = *(float *)this->datain + *(float *)this->dataout;
                    memcpy(this->dataout, (void*)&agg_result, 4);
                    break;
                }
                case FLOAT64_TC: {
                    double agg_result = *(double *)this->datain + *(double *)this->dataout;
                    memcpy(this->dataout, (void*)&agg_result, 8);
                    break;
                }
                default:
                    return false;
            }
            return true;   
        }
        /**
         * @brief aggrerate_method handler
         * 
         */
        void aggrerate_method_handler(int64_t hash_count){
            for(int j = 0; j < hash_count; j++){
                for(int k = 0; k < aggrerate_num; k++){
                    auto datain = tmp_result.get_RC(j, aggrerate_off[k]);
                    float avg_result = 0;
                    switch(this->aggrerate_method[k]){
                        case COUNT:
                            this->aggrerate_type[k]->copy(datain, &(this->pattern_count_ptr[j]));
                            break;
                        case AVG:
                            avg_result = *(float *)this->datain;
                            memcpy(this->datain, (void*)&avg_result, 4);
                            break;
                        default:
                            break;
                    }            
                }
            }
        }

        /**
         * @brief 
         */
        void max_handler(){
            if(this->aggrerate_type[this->agg_i]->cmpLT(dataout, datain)){
                this->aggrerate_type[this->agg_i]->copy(dataout, datain);                  
            }    
        }

        /**
         * @brief 
         */
        void min_handler(){
            if(this->aggrerate_type[this->agg_i]->cmpLT(datain, dataout))
                this->aggrerate_type[this->agg_i]->copy(dataout, datain);            
        }
};

class Executor {
    private:
        Operator    *top_op = NULL;  /**< Top operator of the operator tree.   */
        BasicType   **result_type;   /**< Type of every column in result table.*/
        int64_t     *filter_tid;     /**< Table id of filter conditions.       */
        Condition   **join_cond;     /**< Conditions for join.                 */
        int64_t     count;           /**< count records for debug uses         */
        int64_t     timesin;         /**< count for enter func exec times      */
		int64_t     *having_tid;     /**< inside class use                     */
        int64_t     *joinA_tid;      /**< inside class use                     */
        int64_t     *joinB_tid;      /**< inside class use                     */
        int         join_count;      /**< inside class use                     */
    public:
        /**
         * @brief exec function.
         * @param  query to execute, if NULL, execute query at last time 
         * @result result table generated by an execution, store result in pattern defined by the result table
         * @retval >0  number of result rows stored in result
         * @retval <=0 no more result
         */
        virtual int exec(SelectQuery *query, ResultTable *result);

        /**
         * @brief close function.
         * @param None
         * @retval ==0 succeed to close
         * @retval !=0 fail to close
         */
        virtual int close();

    private:
        /**
         * @brief count join link number 
         * @param selected query
         */
        void count_join_link_number(SelectQuery *query){
            for(int i = 0; i < query->where.condition_num; i++){
                if(query->where.condition[i].compare == LINK) {
                this->join_cond[this->join_count] = &query->where.condition[i];
                
                
                Column* joinA_col= (Column *)g_catalog.getObjByName(query->where.condition[i].column.name);
                int64_t joinA_cid = joinA_col->getOid();
                
                Column* joinB_col= (Column *)g_catalog.getObjByName(query->where.condition[i].value);
                int64_t joinB_cid = joinB_col->getOid();
                
                for(int j = 0; j < query->from_number; j++){
                    Table* table = (Table *)g_catalog.getObjByName(query->from_table[j].name);
                    
                    auto columns = table->getColumns();
                    for(int k = 0; k < (int)columns.size(); k++){
                        if(joinA_cid == columns[k]){
                            this->joinA_tid[this->join_count] = j;
                        }
                        if(joinB_cid == columns[k]){
                            this->joinB_tid[this->join_count] = j;
                        }
                    }
                }
                this->join_count++;
                }
            }            
        } 

        /**
         * @brief find tables that filter columns belong to
         * @param selected query
         */
        void find_table(SelectQuery *query){
            for(int i = 0; i <query->where.condition_num; i++){
                if(query->where.condition[i].compare != LINK){

                    Column* filter_col= (Column *)g_catalog.getObjByName(query->where.condition[i].column.name);
                    int64_t filter_cid = filter_col->getOid();

                    for(int j = 0; j < query->from_number; j++){
                        Table* table = (Table *)g_catalog.getObjByName(query->from_table[j].name);
                        auto columns = table->getColumns();
                        for(int k = 0; k < (int)columns.size(); k++){
                            if(filter_cid == columns[k]){
                                this->filter_tid[i] = table->getOid();
                                break; 
                            }
                        }
                    }
                }
            }            
        }

        /**
         * @brief find_having_conditions
         * @param selected query
         */
        void find_having_conditions(SelectQuery *query){
            for (int i = 0; i < query->having.condition_num; i++) {

                Column* having_col= (Column *)g_catalog.getObjByName(query->having.condition[i].column.name);
                int64_t having_cid = having_col->getOid();

                for(int j = 0; j < query->from_number; j++){
                    Table* table = (Table *)g_catalog.getObjByName(query->from_table[j].name);
                    auto columns = table->getColumns();
                    for(int k = 0; k < (int)columns.size(); k++){
                        if(having_cid == columns[k]){
                            this->having_tid[i] = table->getOid();
                            break; 
                        }
                    }
                }
            }            
        }

        /**
         * @brief build_op_tree
         * @param selected query, operator
         */
        void build_op_tree(Operator **Op, SelectQuery *query){
            for(int i = 0; i < query->from_number; i++){

                RowTable *row_table = (RowTable *)g_catalog.getObjByName(query->from_table[i].name);
                Op[i] = new Scan(query->from_table[i].name);

                int64_t row_tid = row_table->getOid();
                for(int j = 0; j < 4; j++){
                    if(this->filter_tid[j] == row_tid) {
                        Op[i] = new Filter(Op[i], &query->where.condition[j]); 
                    }
                    if(having_tid[j] == row_tid) {
                        Op[i] = new Filter(Op[i], &query->having.condition[j]); 
                    }
                }
            }       

            Operator *newop;
            printf("DEBUG 55    %d\n", this->join_count);
            if(this->join_count>1) {
                Operator ***hash_op = new Operator **[4];
                for (int i = 0; i < this->join_count; i++) {
                    hash_op[i] = new Operator*[2];
                    hash_op[i][0] = Op[this->joinA_tid[i]];
                    hash_op[i][1] = Op[this->joinB_tid[i]];
                    printf("DEBUG 56 %d %d %d \n", i, this->joinA_tid[i], this->joinB_tid[i]);
                    newop = new HashJoin(2, hash_op[i], 1, this->join_cond[i]);
                    
                    Op[this->joinA_tid[i]] = newop;
                    Op[this->joinB_tid[i]] = newop;
                }
            //  return false;
            }
            else if (this->join_count == 1) {
                newop = new HashJoin(2, Op, 1, this->join_cond[0]);
            }
            else newop = Op[0];
            
            if(query->select_number)
                newop = new Project(newop, query->select_number, query->select_column);
            if(query->groupby_number)
                newop = new GroupBy(newop, query->select_number, query->select_column);
            if(query->orderby_number)
                newop = new OrderBy(newop, query->orderby_number, query->orderby);
            //---------------------init the operator tree ---------------------
            top_op = newop;
                top_op->init();     
        }

};
#endif
