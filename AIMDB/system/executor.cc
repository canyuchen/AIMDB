#include "executor.h"
using namespace std;
int64_t project_tabout_id = 456;
int64_t tabout_id_hashjoin = 777;

char ** hashjoin_cmpSrcB_tables = new char* [2];
char * hashjoin_format_value = new char[128]; //Question

//------------structure of our operator tree-----------
/*                      result_table
                              |
                           orderby
                             |
                        having(filter)
                             |
                          groupby
                            |
                          project
                            |
                       join(maybe not)
                           |
                         filter
                           |
                          scan


*/

/** read row to result table */
bool ReadRowToResult(RowTable *src, int64_t row_rank, int64_t col_num, ResultTable *dest) {
    int current_col;
    char * buffer;
    g_memory.alloc(buffer, 128);
    for (current_col = 0; current_col < col_num; current_col++) {
        if (!src->selectCol(row_rank, current_col, buffer))
        {
            g_memory.free(buffer ,128);
            return false;
        }
        if (!dest->write_RC(0, current_col, buffer))
        {
            g_memory.free(buffer ,128);
            return false;
        }
    }
    g_memory.free(buffer ,128);
    return true;
}

/** transfer const char to char   */
char *constchar2char(const char *str) {
    size_t len = strlen(str);
    char *result = new char[len + 1];
    memcpy(result, str, len + 1);
    return result;
}

/** transfer int to string format */
string int2str(int64_t x) {
    string s = to_string(x);
    return(s);
}
/** get a number around size,powdered by 2 */
int64_t round2(int64_t size) {
    if (size < 0) return -1;
    int64_t result = 1;
    while (result < size)
        result *= 2;
    return result;
}

/** get hash number */
uint32_t gethash(char *key, BasicType * type) {
    uint32_t hash = 0;
    for (unsigned  i = 0; i < strlen(key); i++)
        hash = 31 * hash + key[i];
    return hash;
}

/** exeutor function */
int Executor::exec(SelectQuery *query, ResultTable *result)
{
    if(query != NULL) {
		int join_count = 0; // number of join req
		count = 0;          // number of records 
		timesin= 0;         // times comming in this function

		int64_t filter_tid[4] = { -1, -1, -1, -1 }; // init of filter 
		int64_t having_tid[4] = { -1, -1, -1, -1 }; // init of having
        int64_t joinA_tid[4] = { -1, -1, -1, -1 };
        int64_t joinB_tid[4] = { -1, -1, -1, -1 };
		Condition *join_cond[4];                    // condtions 

		this->filter_tid = filter_tid; // init of filter 
		this->having_tid = having_tid; // init of having
        this->joinA_tid = joinA_tid;
        this->joinB_tid = joinB_tid;
        this->join_cond = join_cond;
        this->join_count = join_count;

		this->count_join_link_number(query);
		
        this->find_table(query);

        this->find_having_conditions(query);

		// build the operator tree
		Operator *Op[4];

        this->build_op_tree(Op, query);
	}
    // build result table to store result
    int col_num = (int) top_op->getTableOut()->getColumns().size();
    timesin ++;
    auto row_pattern =top_op->getTableOut()->getRPattern();
    result_type = new BasicType *[col_num];
    for(int i=0; i < col_num; i++)
        result_type[i] = row_pattern.getColumnType(i);

    //clear result table
    if (timesin > 1)
        result->shut();

    result->init(result_type, col_num, 2048); 
    ResultTable final_tmp_result;
    result->row_number = 0; 
    final_tmp_result.init(result_type, col_num , 2048); 

    // write result table 
    while(result->row_number < 1024/result->row_length && top_op->get_Next(&final_tmp_result)){  
        for(int j = 0; j < col_num; j++){
            char* buf = final_tmp_result.get_RC(0, j);
            result->write_RC(result->row_number, j, buf);
        }
        result->row_number ++;
    }
    count += result->row_number;

    // 	ENDS return and close  
    if(result->row_number == 0) {
        top_op->close();
        printf("record count %ld\n",count);
        return false;
    }
    return true;
}

int Executor::close() 
{
    return 0;
}

// note: you should guarantee that col_types is useable as long as this ResultTable in use, maybe you can new from operate memory, the best method is to use g_memory.
int ResultTable::init(BasicType *col_types[], int col_num, int64_t capicity) {
    column_type = col_types;
    column_number = col_num;
    row_length = 0;
    buffer_size = g_memory.alloc (buffer, capicity);
    if(buffer_size != capicity) {
        printf ("[ResultTable][ERROR][init]: buffer allocate error!\n");
        return -1;
    }
    int allocate_size = 1;
    int require_size = sizeof(int)*column_number; 
    while (allocate_size < require_size)
        allocate_size = allocate_size << 1;
    if(allocate_size < 8)
    allocate_size *=2;
    char *p = NULL;
    offset_size = g_memory.alloc(p, allocate_size);
    if (offset_size != allocate_size) {
        printf ("[ResultTable][ERROR][init]: offset allocate error!\n");
        return -2;
    }
    offset = (int*) p;
    for(int ii = 0;ii < column_number;ii ++) {
        offset[ii] = row_length;
        row_length += column_type[ii]->getTypeSize(); 
    }
    row_capicity = (int)(capicity / row_length);
    row_number   = 0;
    return 0;
}

/** print */
int ResultTable::print (void) {
    int row = 0;
    int ii = 0;
    char buffer[1024];
    char *p = NULL; 
    while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = get_RC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            printf("%s\t", buffer);
        }
        p = get_RC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        printf("%s\n", buffer);
        row ++; ii=0;
    }
    return row;
}

/** dump result to fp*/
int ResultTable::dump(FILE *fp) {
    int row = 0;
    int ii = 0;
    char *buffer;
    g_memory.alloc(buffer, 128);
    char *p = NULL; 
   while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = get_RC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            fprintf(fp,"%s\t", buffer);
        }
        p = get_RC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        fprintf(fp,"%s\n", buffer);
        row ++; ii=0;
    }
    g_memory.free(buffer,128);
    return row;
}

// this include checks, may decrease its speed
char* ResultTable::get_RC(int row, int column) {
    return buffer+ row*row_length+ offset[column];
}

/** write rc with data*/
int ResultTable::write_RC(int row, int column, void *data) {
    char *p = get_RC (row,column);
    if (p==NULL) return 0;
    return column_type[column]->copy(p,data);
}

/** shut memory */
int ResultTable::shut (void) {
    // free memory
    if (buffer) {
        g_memory.free (buffer, buffer_size);
    }
    if (offset) {
        g_memory.free ((char*)offset, offset_size);
    }
    return 0;
}

//---operators implementation---

//-  ---------Scan--------------
Scan::Scan(char*tablename) {
    RowTable *table= (RowTable *)g_catalog.getObjByName(tablename);
    this->table_in[0] = table;
    this->table_out = table;
    this->col_num = table->getColumns().size();
}

bool Scan::init(void) {
    this->current_row = 0;
    return true;
}

bool Scan::get_Next(ResultTable *result) {
    if (is_End()) return false;
    char* buffer ;
    g_memory.alloc(buffer, 128);
    if(!this->writeRow(buffer, result)){
        return false;
    }
    this->current_row++;
    g_memory.free(buffer, 128);
    return true;
}

bool Scan::is_End(void) {
    return this->equalornot();
}

bool Scan::close() { return true; }

//----------Filter--------------
Filter::Filter(Operator *Op, Condition *condi) {
    this->prior_op = Op;
    this->compare_method = condi->compare;
    this->table_in[0] = prior_op->getTableOut();
    this->table_out = this->table_in[0];
    
    this->resinit();
    
    Object *col = g_catalog.getObjByName(condi->column.name);
    this->col_rank = this->table_out->getColumnRank(col->getOid());
    this->value_type = this->table_out->getRPattern().getColumnType(this->col_rank);
    this->value_type->formatBin(this->value, condi->value); //get fixed value
}

bool Filter::init() {
    return prior_op->init();
}

bool Filter::get_Next(ResultTable *result) {
    bool flag = false;
    char * DEBUG_A;
    g_memory.alloc(DEBUG_A, 128);
    char * DEBUG_B;
    g_memory.alloc(DEBUG_B, 128);
    while (!flag) {
        if (prior_op->is_End()) break;
        if (!prior_op->get_Next(&this->result)) return false;
        char* cmpSrcA_ptr = this->result.get_RC(0, this->col_rank); //variable value
        char* cmpSrcB_ptr = this->value; //fixed value
        this->value_type->formatTxt(DEBUG_A, cmpSrcA_ptr);
        this->value_type->formatTxt(DEBUG_B, cmpSrcB_ptr);
        
        flag = this->compare_exec(cmpSrcA_ptr, cmpSrcB_ptr);
    }
    if(!this->resCopy(flag, result)){
        return false;
    }
        g_memory.free(DEBUG_A, 128);
        g_memory.free(DEBUG_B, 128);
    return flag;
}

bool Filter::is_End(){
    return prior_op->is_End();
}

bool Filter::close(){
            bool tmp = prior_op->close();
            this->MemShut();
            return tmp;
}

//--------Project-------
Project::Project(Operator *Op, int64_t col_tot, RequestColumn *cols_name) {
    this->prior_op = Op;
    this->col_tot = col_tot;
    this->table_in[0] = Op->getTableOut();
    this->in_RP = Op->getTableOut()->getRPattern();
    int in_colnum = Op->getTableOut()->getColumns().size();
    this->ResInit(in_colnum);
    
    for (int i = 0; i < col_tot; i++) {
        Object *col = g_catalog.getObjByName(cols_name[i].name);
        this->col_rank[i] = Op->getTableOut()->getColumnRank(col->getOid());
    }
    
    string cname_head = "tmp_project_table";
    char *cname = constchar2char(strcat(constchar2char(cname_head.c_str()), to_string(project_tabout_id).c_str()));
    
    RowTable *project_table = (RowTable *)g_catalog.getObjByName(cname);
    if (project_table != NULL){
        project_table->shut();
    } 
    table_out = new RowTable(project_tabout_id++, cname); 
    table_out->init();
    RPattern *new_RPattern = &this->table_out->getRPattern();
    RPattern *old_RPattern = &Op->getTableOut()->getRPattern();
    auto &cols_id = table_in[0]->getColumns();
    new_RPattern->init(col_tot);
    for (int i = 0; i < col_tot; i++) {
        new_RPattern->addColumn(old_RPattern->getColumnType(col_rank[i]));
        table_out->addColumn(cols_id[col_rank[i]]);
    }
}

bool Project::init(){
    return prior_op->init();
}
bool Project::get_Next(ResultTable *result) {
    if (is_End()) return false;
    if(!this->WriteRow(result)){
        return false;
    }
    return true;
}
bool Project::is_End(){
    return prior_op->is_End();
}
bool Project::close(){
            bool tmp = prior_op->close();
            this->ShutMem();
            return tmp;
}

//----------HashJoin----------

HashJoin::HashJoin(int operator_num, Operator **Op, int cond_num, Condition *condi) {
    this->Fdeal(operator_num, Op);

    char * col_a;
    char * col_b;
    col_a = condi->column.name;
    col_A_oid = g_catalog.getObjByName(col_a)->getOid();
    col_b = condi->value;
    col_B_oid = g_catalog.getObjByName(col_b)->getOid();
    
    this->MakeOrder();
    //make up a temporary table_out
    string cname_head = "tmp_hashjoin_table";
    char *cname = constchar2char(strcat(constchar2char(cname_head.c_str()), to_string(tabout_id_hashjoin).c_str()));
    RowTable *hashjoin_table = (RowTable *)g_catalog.getObjByName(cname);
    if (hashjoin_table != NULL) hashjoin_table->shut();
    table_out = new RowTable(tabout_id_hashjoin++, cname); 
    table_out->init();
    
    RPattern *new_RPattern = &table_out->getRPattern();
    RPattern *old_RPattern = NULL;
    
    new_RPattern->init(table_out_col_num);
    
    in_col_type = new BasicType *[table_out_col_num];
    BasicType *t;  
    int cnt = 0;
    for (int i = 0; i < operator_num; i++) {
        old_RPattern = &table_in[i]->getRPattern();
        auto &cols_id = table_in[i]->getColumns();
        for (int j = 0; j < col_num[i]; j++) {
            in_col_type[cnt++] = old_RPattern->getColumnType(j);
            t = old_RPattern->getColumnType(table_in[i]->getColumnRank(cols_id[j]));
            new_RPattern->addColumn(t);
            table_out->addColumn(cols_id[j]);
        }
    }
    this->result.init(in_col_type, col_num[0]);
}

bool HashJoin::init(void) {
    for(int i = 0; i <operator_num; i++)
    if (!Op[i]->init()) return false;
    col_B_type = new BasicType * [col_num[1]];
    for (int i=0; i < col_num[1]; i++)
        col_B_type[i] = table_in[1]->getRPattern().getColumnType(i);
    
    char * value;
    char * format_value;
    g_memory.alloc(format_value,128);
    hash_table = new HashTable(200000, 10, 0);
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
    g_memory.free(format_value, 128);
    
    return true;
}

bool HashJoin::get_Next(ResultTable *result) {
    bool flag = false;
    char* cmpSrcA_ptr;
    char* cmpSrcB_ptr;
    ResultTable * row_i;
    while (!flag) {
        if (Op[0]->is_End() || !Op[0]->get_Next(&this->result)) break;
        cmpSrcA_ptr = this->result.get_RC(0, col_A_rank); 
        
        BasicType * value_type = table_in[0]->getRPattern().getColumnType(col_A_rank);
        
        value_type->formatTxt(hashjoin_format_value, cmpSrcA_ptr);
        
        uint32_t hash_result = gethash(hashjoin_format_value, value_type);
        int64_t cmpSrcB_table_num = hash_table->probe(hash_result, hashjoin_cmpSrcB_tables, 2);
        for (int i=0; i<cmpSrcB_table_num; i++) {
            row_i = (ResultTable *) hashjoin_cmpSrcB_tables[i];
            cmpSrcB_ptr = row_i->get_RC(0, col_B_rank);
            value_type->formatTxt(hashjoin_format_value, cmpSrcB_ptr);
            if (this->value_type->cmpEQ(cmpSrcA_ptr, cmpSrcB_ptr)) {
                flag = true;
                break;
            }
        }
        if (flag) break;
    }
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
    return flag;
}

bool HashJoin::close(void) {
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

bool HashJoin::is_End(void)  {
    return Op[0]->is_End();
}




//------------------OrderBy------------------------------

OrderBy::OrderBy(Operator *op_ptr, int64_t order_by_num, RequestColumn *cols_name) {
    this->prior_op = op_ptr;
    this->table_in[0] = this->get_prior_operator()->getTableOut();
    this->table_out = this->get_prior_operator()->getTableOut();
    this->col_num = this->get_prior_operator()->getTableOut()->getColumns().size();
    this->order_by_num = order_by_num;
    this->rpattern = this->get_prior_operator()->getTableOut()->getRPattern();
    in_col_type = new BasicType *[this->col_num];
    int64_t in_col_num = this->get_prior_operator()->getTableOut()->getColumns().size();
    this->init_col_type();
    this->result.init(in_col_type, in_col_num);
    this->re.init(in_col_type, in_col_num, 1024*32);

    this->row_length =this->get_rpattern().getRowSize(); 

    this->in_cols_name = cols_name;
    this->init_col();
}
bool OrderBy::init(){
    char* buffer;
    int i = 0;
    this->get_prior_operator()->init();
    while(this->check_not_end())
    {
        this->write_result_col(i, buffer);
        i++;
    }
    this->record_size = i;  
    quick_sort(re.buffer, 0, record_size-1);
    index= 0;
    return true;
}

void OrderBy::quick_sort(char *buffer, int64_t left, int64_t right) {
    if(left < right)
    {
        char *key =(char*) malloc(this->row_length) ;
        memcpy( key, buffer + left*this->row_length, this->row_length);
        int64_t low =left;
        int64_t high = right;
        this->low = low;
        this->high = high;
        this->key = key;
        this->qsort_helper(buffer);
        memcpy(buffer+this->low * this->row_length, this->key, this->row_length);
        quick_sort(buffer,left, this->low-1);
        quick_sort(buffer,this->low+1, right);
        free(key);
    }
}

int OrderBy::cmp(void*l, void*r)
{    int i;
    for(i = 0;i < order_by_num ;)
    {
        auto p =l+compare_col_offset[i];
        auto q =r+compare_col_offset[i];
        if(this->compare_col_type[i]->cmpEQ(p, q)){
            i++;            
        }
        else{
            return (this->compare_col_type[i]->cmpGT(p, q)) ? 2 : 0;
        }
    }
    return 1;
}

bool OrderBy::get_Next(ResultTable *result)
{
    if(this->is_End())return false;
    char *buffer;
    if(!this->check_stop_get_Next(result, buffer)){
        return false;
    }
    this->index ++;
    return true;
}   

bool OrderBy::is_End(){
    return index == record_size;
} 

bool OrderBy::close(){
            int t = prior_op->close(); 
            delete prior_op;
            result.shut();
            re.shut();
            delete [] in_col_type;
            return t;
}
//----------------GroupBy-----------------

GroupBy::GroupBy(Operator *Op, int groupby_num, RequestColumn req_col[4]){
    this->prior_op = Op;
    this->table_in[0] = this->get_prior_tableout();
    this->table_out = this->get_prior_tableout();  
    this->rpattern = this->get_prior_rpattern();
    this->row_size = this->get_rpattern().getRowSize();
    this->col_num = this->table_out->getColumns().size();
    this->req_col_ptr = req_col;
    for(int i = 0; i < groupby_num; i++){
        if(req_col[i].aggrerate_method == NONE_AM){
            this->init_non_aggre(i);
        }
        else{
            this->init_aggre(i);
        }
    }
}

bool GroupBy::init(){
    if(!prior_op->init())
        return false;
    //------Result Tables------
    RPattern in_RP = table_in[0]->getRPattern();
    int in_colnum = table_in[0]->getColumns().size();
    BasicType   **in_col_type;
    in_col_type = new BasicType *[in_colnum];
    for(int i=0; i < in_colnum; i++){
        in_col_type[i] = in_RP.getColumnType(i);        
    }
    this->tmp_result.init(in_col_type, in_colnum,524288);
    this->tmp_one.init(in_col_type, in_colnum);    
    //------hash table------
    HashTable *hstable = new HashTable(1000000, 4, 0); 
    int64_t hash_count = 0;

    char **probe_result = (char**)malloc(4 * round2(row_size));
    char *src;
    int64_t tmp_offset = 0;
    int64_t pattern_count[32768];
    int64_t key;
    char tmp_buffer[1024];

    for(int i = 0; i < 32768; i++)
        pattern_count[i] = 0;
    while( !prior_op->is_End()&&prior_op->get_Next(&tmp_one)){
        //------generate the hash key------
        key = 0;
        tmp_offset = 0;
        for(int i = 0; i < non_aggrerate_num; i++){
            src = tmp_one.get_RC(0, non_aggrerate_off[i]);
            tmp_one.column_type[non_aggrerate_off[i]]->formatTxt(tmp_buffer+tmp_offset, src);
            tmp_offset += non_aggrerate_type[i]->getTypeSize();
        }
        key = gethash(tmp_buffer, NULL);   
        //-----look up in hashtable-----
        if(hstable->probe(key, probe_result, 4) > 0){
            pattern_count[(uint64_t)probe_result[0]]++;
            for(int i = 0; i < aggrerate_num; i++){
                aggrerate(aggrerate_method[i], (uint64_t)probe_result[0], i);
            }
        }
        else{
            hstable->add(key, (char*)hash_count);
            for(int j = 0; j < col_num; j++){
                src = tmp_one.get_RC(0, j);
                tmp_result.write_RC(hash_count, j, src);             
            }
            tmp_result.row_number++;   
            pattern_count[hash_count]++;
            hash_count++;
        }
    }
    this->pattern_count_ptr = pattern_count;
    this->aggrerate_method_handler(hash_count);
    free(probe_result);
    return true;
}


bool GroupBy::get_Next(ResultTable *result){
    if(this->index >= tmp_result.row_number)
        return false;
    for(int j = 0; j < col_num; j++){
        char* buf = tmp_result.get_RC(this->index, j);
        if(!result->write_RC(0, j, buf)) return false;
    }
    index++;
    return true;
}

bool GroupBy::is_End(){
    return prior_op->is_End();
}

bool GroupBy::close() {
    bool tmp = prior_op->close();
    delete prior_op;
    tmp_one.shut();
    tmp_result.shut();
    return tmp;
}

bool GroupBy::aggrerate(AggrerateMethod method, uint64_t probe_result, int agg_i){
    this->agg_i = agg_i;
    char* datain = tmp_one.get_RC(0, aggrerate_off[this->agg_i]);
    char* dataout = tmp_result.get_RC(probe_result, aggrerate_off[this->agg_i]);
    this->datain = datain;
    this->dataout = dataout;
    switch(method){
    case SUM:
        this->sum_handler();
    case AVG:{
        return this->avg_handler();
    }
    case MAX:
        this->max_handler();
        break;
    case MIN:
        this->min_handler();
        break;
    default:
        break;
    }
}
