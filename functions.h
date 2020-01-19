#ifndef _FUNCTIONS_H
#define _FUNCTIONS_H
#include <iostream>
#include <cstdlib>
#include <cstring>
#include "list.h"
#include <ctime>
#include <cstdio>
#include <math.h>
#include "JobScheduler.h"
#include <inttypes.h>
#include "besttreemap.h"


#define MAX_INPUT_FILE_NAME_SIZE 100
#define MAX_INPUT_ARRAYS_NUM 20
#define MAX_BOOLEAN_ARRAY_SIZE 50000000 // max size of the array that is used to find distinct values of a column of InputArray

enum RunningMode {serial = 0, parallel = 1};

// parallelism modes
extern RunningMode queryMode;
extern RunningMode reorderMode;
extern RunningMode quickSortMode;
extern RunningMode joinMode;
extern RunningMode projectionMode;
extern RunningMode filterMode;

// parallelism mode flags
extern bool newJobPerBucket; // if == true and reorder mode is parallel: a new Job is created per bucket 
extern bool OptimizePredicatesFlag; // if == true: join enumeration is used
extern bool jthreads; // if == true: parallel join is performed by equally splitting the result for each thread to handle

typedef class list list;

// thread synchronization variables
// used for synchronization of jobs of each predicate of the currently handled query
extern pthread_mutex_t *predicateJobsDoneMutexes;
extern pthread_cond_t *predicateJobsDoneConds;
extern pthread_cond_t *jobsCounterConds;
// used for synchronization of query jobs of the currently handled batch
extern pthread_mutex_t queryJobDoneMutex;
extern pthread_cond_t queryJobDoneCond;
extern int available_threads; // used to prevent deadlock when queries AND other tasks are parallel AND the number of threads is smaller than the number of queries of the currently handled batch
extern bool** lastJobDoneArrays; // 2-dimensional array with size = queries count X 2 
                                 // e.g. if lastJobDoneArrays[queryIndex][0] == true: all reorder and quicksort jobs of the 1st array of the currently handled predicate are scheduled
extern int queryJobDone; // count of query jobs that are currenlty running
extern char** QueryResult; // used for storing results of each parallel query
extern JobScheduler *scheduler; // global scheduler object

class tuple
{
public:
    uint64_t key;
    uint64_t payload;
};

const unsigned long BUCKET_SIZE = 64 * pow(2, 10);  //64KB (I think)
const unsigned long TUPLE_SIZE = sizeof(tuple);
const int TUPLES_PER_BUCKET = (int)(BUCKET_SIZE / TUPLE_SIZE);  
const uint64_t power=pow(2,8);
//each bucket must be smaller than 64KB 
//size of bucket = num_tuples * sizeof(tuples)  
//num_tuples (of each bucket) = 64KB / sizeof(tuple)

class relation
{
public:
    tuple* tuples;
    uint64_t num_tuples;
    void print();
    relation();
    ~relation();
};

class result
{
public:
    list* lst;
};

// forward declaration
class InputArray;

// stores statistics of a column of input array
class ColumnStats {
    public:
        uint64_t minValue, maxValue, valuesNum, distinctValuesNum;
        bool changed;

        ColumnStats() {
            minValue = 0;
            maxValue = 0;
            valuesNum = 0;
            distinctValuesNum = 0;
            changed = false;
        }

        void calculateDistinctValuesNum(const InputArray* inputArray, uint64_t columnIndex);
};

// InputArray is used for:
// 1. Storing the data of each input relation
// 2. Storing only the row ids of the initial input relation
class InputArray
{
    public:
    uint64_t rowsNum, columnsNum; // rowsNum: rows count, columnsNum: columns count
    uint64_t** columns; // data of the input array OR row ids of the input array depending on the usage of this InputArray
    ColumnStats* columnsStats; // one ColumnStats object for each column of the input array

    InputArray(uint64_t rowsNum, uint64_t columnsNum); // initialization for storing data of input array
    InputArray(uint64_t rowsNum);  // initialization for storing row ids of input array
    ~InputArray();

    // filtering when storing row ids
    InputArray* filterRowIds(uint64_t fieldId, int operation, uint64_t numToCompare,const InputArray* pureInputArray, uint64_t startIndex, uint64_t stopIndex); // simple filter
    InputArray* filterRowIds(uint64_t field1Id, uint64_t field2Id,const InputArray* pureInputArray, uint64_t startIndex, uint64_t stopIndex); // inner join
    // fills rel with row ids(keys) and payloads by extracting data from pureInputArray by using the row ids stored in this InputArray
    void extractColumnFromRowIds(relation& rel, uint64_t fieldId,const InputArray* pureInputArray); // column extraction from the initial input array (pureInputArray)
    
    void print();
    // used for columnsStats array initialization
    void initStatistics();
};

// stores intermediate results
class IntermediateArray {
    public:
    uint64_t** results; // contains columns consisted of row ids: one column per used predicate array
    int* inputArrayIds; // size = columnsNum
                        // contains ids of input arrays that correspond to each column
    int* predicateArrayIds; // size = columnsNum
                            // contains ids of predicate arrays that correspond to each column
    uint64_t columnsNum;  // columns count
    uint64_t rowsNum;     // rows count

    IntermediateArray(uint64_t columnsNum);
    ~IntermediateArray();

    // fills resultRelation with row ids of this IntermediateArray and payloads of inputArray that correspond to field with id fieldId
    void extractFieldToRelation(relation* resultRelation, const InputArray* inputArray, int predicateArrayId, uint64_t fieldId);
    // fills this IntermediateArray with data taken from the intermediateResult and, if it is not NULL, the prevIntermediateArray by adding one new column to it
    void populate(uint64_t** intermediateResult, uint64_t rowsNum, IntermediateArray* prevIntermediateArray, int inputArray1Id, int inputArray2Id, int predicateArray1Id, int predicateArray2Id);
    // returns true or false depending on whether the inputArrayId exists in inputArrayIds array
    bool hasInputArrayId(int inputArrayId);
    // returns true or false depending on whether the predicateArrayId exists in predicateArrayIds array
    bool hasPredicateArrayId(int predicateArrayId);
    // returns the index of the inputArrayId in inputArrayIds array or -1
    uint64_t findColumnIndexByInputArrayId(int inputArrayId);
    // returns the index of the predicateArrayId in predicateArrayIds array or -1
    uint64_t findColumnIndexByPredicateArrayId(int predicateArrayId);
    // performs inner-join/self-join of this IntermediateArray by joining row ids of arrays with ids inputArray1Id and inputArray2Id according to their fields with ids field1Id and field2Id
    IntermediateArray* selfJoin(int inputArray1Id, int inputArray2Id, uint64_t field1Id, uint64_t field2Id, const InputArray* inputArray1, const InputArray* inputArray2);
    void print();
};

// forward declarations
void tuplereorder(tuple* array,tuple* array2, int offset,int shift, bool isLastCall, int reorderIndex, int queryIndex);
void handlequery(char** ,const InputArray** , int);
void quickSort(tuple*, int, int, int, int, bool);
void joinparallel(tuple* R, tuple* S, int Rsize, int Sstart, int Send,list* lst,int queryIndex);
void handleprojectionparallel(IntermediateArray* rowarr,const InputArray** array,int projarray,int predicatearray,int projcolumn,char* buffer,int queryIndex);

// Job for parallel queries
class queryJob : public Job {
    
private:
    char** parts;
    const InputArray** allrelations;
    int queryIndex;

public:
    queryJob(char** parts,const InputArray** allrelations, int queryIndex) : Job() 
    { 
        this->parts = parts;
        this->allrelations = allrelations;
        this->queryIndex = queryIndex;
    }

    void run() override
    {
        handlequery(parts, allrelations, queryIndex);
        return; 
    }
};

// Job for parallel reorder
class trJob : public Job
{
private:
    tuple* array;
    tuple* array2;
    int offset;
    int shift;
    bool isLastCall;
    int reorderIndex;
    int queryIndex;

public:
    trJob(tuple* array,tuple* array2, int offset,int shift, bool isLastCall, int reorderIndex, int queryIndex) : Job() 
    { 
        this->array = array;
        this->array2 = array2;
        this->offset = offset;
        this->shift = shift;
        this->isLastCall = isLastCall;
        this->reorderIndex = reorderIndex;
        this->queryIndex = queryIndex;
    }

    void run() override
    {
        tuplereorder(array, array2, offset, shift, isLastCall, reorderIndex, queryIndex);

        pthread_mutex_lock(&jobsCounterMutexes[queryIndex]);
        jobsCounter[queryIndex]--;
        if (jobsCounter[queryIndex] == 0) {
            pthread_cond_signal(&jobsCounterConds[queryIndex]);
        }

        pthread_mutex_unlock(&jobsCounterMutexes[queryIndex]);
        return; 
    }
};

// Job for parallel quicksort
class qJob : public Job
{
private:
    tuple* tuples;
    int startIndex;
    int stopIndex;
    int queryIndex;
    int reorderIndex;
    bool isLastCall;

public:
    qJob(tuple* tuples, int startIndex, int stopIndex, int queryIndex, int reorderIndex, bool isLastCall) : Job()
    {
        this->tuples = tuples;
        this->startIndex = startIndex;
        this->stopIndex = stopIndex;
        this->queryIndex = queryIndex;
        this->reorderIndex = reorderIndex;
        this->isLastCall = isLastCall;
    }

    void run() override
    {
        quickSort(tuples, startIndex, stopIndex, queryIndex, reorderIndex, isLastCall);

        pthread_mutex_lock(&jobsCounterMutexes[queryIndex]);
        jobsCounter[queryIndex]--;
        if (jobsCounter[queryIndex] == 0) {
            pthread_cond_signal(&jobsCounterConds[queryIndex]);
        }
        pthread_mutex_unlock(&jobsCounterMutexes[queryIndex]);
        return; 
    }
};

// Job for parallel join
class jJob : public Job
{
private:
    tuple* R;
    tuple* S;
    int Rsize;
    int Sstart;
    int Send;
    list* lst;
    int queryIndex;
public:
    jJob(tuple* R, tuple* S, int Rsize, int Sstart, int Send,list* lst,int queryIndex) : Job() {this->R=R;this->S=S;this->Rsize=Rsize;this->Sstart=Sstart;this->Send=Send;this->lst=lst;this->queryIndex=queryIndex;}

    void run() override
    {
        joinparallel(R,S,Rsize,Sstart,Send,lst,queryIndex);
        return;
    }
};

// Job for parallel projection/sum calculation
class pJob : public Job
{
private:
    IntermediateArray* rowarr;
    const InputArray** array;
    int projarray;
    int predicatearray;
    int projcolunn;
    char* buffer;
    int queryIndex;
public:
    pJob(IntermediateArray* rowarr,const InputArray** array,int projarray,int predicatearray,int projcolumn,char* buffer,int queryIndex) : Job()
    {
        this->rowarr=rowarr;
        this->array=array;
        this->projarray=projarray;
        this->predicatearray=predicatearray;
        this->projcolunn=projcolumn;
        this->buffer=buffer;
        this->queryIndex=queryIndex;

    }
    void run() override
    {
        handleprojectionparallel(rowarr,array,projarray,predicatearray,projcolunn,buffer,queryIndex);
        return;

    }
};

// Job for parallel filter of InputArray
class filterJob : public Job
{
private:
    uint64_t fieldId;
    int operation;
    uint64_t numToCompare;
    const InputArray* pureInputArray;
    uint64_t startIndex;
    uint64_t stopIndex;
    InputArray* oldInputArrayRowIds;
    InputArray** newInputArrayRowIdsP;
    int queryIndex;

public:
    filterJob(uint64_t fieldId, int operation, uint64_t numToCompare, const InputArray* pureInputArray, uint64_t startIndex, uint64_t stopIndex, InputArray* oldInputArrayRowIds, InputArray** newInputArrayRowIdsP, int queryIndex) : Job()
    {
        this->fieldId = fieldId;
        this->operation = operation;
        this->numToCompare = numToCompare;
        this->pureInputArray = pureInputArray;
        this->startIndex = startIndex;
        this->stopIndex = stopIndex;
        this->oldInputArrayRowIds = oldInputArrayRowIds;
        this->newInputArrayRowIdsP = newInputArrayRowIdsP;
        this->queryIndex = queryIndex;
    }

    void run() override
    {
        (*newInputArrayRowIdsP) = oldInputArrayRowIds->filterRowIds(fieldId, operation, numToCompare, pureInputArray, startIndex, stopIndex);

        pthread_mutex_lock(&jobsCounterMutexes[queryIndex]);
        
        jobsCounter[queryIndex]--;
        if (jobsCounter[queryIndex] == 0) {
            pthread_cond_signal(&jobsCounterConds[queryIndex]);
        }

        pthread_mutex_unlock(&jobsCounterMutexes[queryIndex]);

        return;
    }
};

// Job for parallel inner-join/self-join of InputArray
class innerJoinJob : public Job
{
private:
    uint64_t field1Id;
    uint64_t field2Id;
    const InputArray* pureInputArray;
    uint64_t startIndex;
    uint64_t stopIndex;
    InputArray* oldInputArrayRowIds;
    InputArray** newInputArrayRowIdsP;
    int queryIndex;

public:
    innerJoinJob(uint64_t field1Id, uint64_t field2Id, const InputArray* pureInputArray, uint64_t startIndex, uint64_t stopIndex, InputArray* oldInputArrayRowIds, InputArray** newInputArrayRowIdsP, int queryIndex) : Job()
    {
        this->field1Id = field1Id;
        this->field2Id = field2Id;
        this->pureInputArray = pureInputArray;
        this->startIndex = startIndex;
        this->stopIndex = stopIndex;
        this->oldInputArrayRowIds = oldInputArrayRowIds;
        this->newInputArrayRowIdsP = newInputArrayRowIdsP;
        this->queryIndex = queryIndex;
    }

    void run() override
    {
        (*newInputArrayRowIdsP) = oldInputArrayRowIds->filterRowIds(field1Id, field2Id, pureInputArray, startIndex, stopIndex);

        pthread_mutex_lock(&jobsCounterMutexes[queryIndex]);
        
        jobsCounter[queryIndex]--;
        if (jobsCounter[queryIndex] == 0) {
            pthread_cond_signal(&jobsCounterConds[queryIndex]);
        }

        pthread_mutex_unlock(&jobsCounterMutexes[queryIndex]);

        return;
    }
};

// used for combination of the results of parallel inner-join or filter
InputArray* combineInputArrayRowIds(InputArray** inputArrayRowIdsParts, int partsNum);
// used by parent thread to wait for all the jobs of the current part of the program to finish in order to proceed
void waitForJobsToFinish(int queryIndex);
// handles parallel filter or inner join depending on the isFilterJob variable and returns the combined result
InputArray* parallelFilterOrInnerJoin(int queryIndex, InputArray* inputArrayRowIds, bool isFilterJob, int field1Id, int field2Id, int operation, uint64_t numToCompare, const InputArray* pureInputArray);
// shifts payload by "shift" number of bytes
uint64_t hashFunction(uint64_t payload, int shift);
// performs join between 2 relations and returns the result
result* join(relation* R, relation* S,uint64_t**r,uint64_t**s,int rsz,int ssz,int joincol);
// handles reorder of array by taking into consideration the parallelism modes of reorder and quicksort
void tuplereorder(tuple* array,tuple* array2, int offset,int shift, bool isLastCall, int reorderIndex, int queryIndex);

// functions for quick sort
void swap(tuple* tuple1, tuple* tuple2);
int randomIndex(int startIndex, int stopIndex);
int partition(tuple* tuples, int startIndex, int stopIndex);
void quickSort(tuple* tuples, int startIndex, int stopIndex, int queryIndex, int reorderIndex, bool isLastCall);
// reads input arrays from binary file and stores their data in an array of InputArrays which it returns
// also calculates and stores the statistics for each column of each input array
InputArray** readArrays();
char** readbatch(int& lns);
char** makeparts(char* query);
void handlequery(char** parts,const InputArray** allrelations, int queryIndex);
// populates relationIds array with the array ids of the currently handled query
void loadrelationIds(int* relationIds, char* part, int& relationsnum);
// returns true or false depending on whether the next joined array should be sorted
bool shouldSort(uint64_t** predicates, int predicatesNum, int curPredicateIndex, int curPredicateArrayId, int curFieldId, bool prevPredicateWasFilterOrSelfJoin);
// returns true or false depending on whether next joined array should be sorted
// if the array should be sorted, tuplereorder() is called to sort it
bool handleReorderRel(relation* rel, tuple** t, uint64_t** preds, int cntr, int i, int predicateArrayId, int fieldId, bool prevPredicateWasFilterOrSelfJoin, int queryIndex, int reorderIndex);
// used by parent thread to wait until all the reorder and quicksort jobs are scheduled in order to proceed
void waitForReorderJobsToBeQueued(int queryIndex, int reorderIndex);
// handles deletion of preds, inputArraysRowIds and rslt
// if onlyResult == true, only rslt is deleted
void handleDelete(uint64_t** preds, int cntr, int relationsnum, InputArray** inputArraysRowIds, result* rslt, bool onlyResult);
// performs all filters and joins of the currently handled query
// returns the result in an IntermediateArray pointer
IntermediateArray* handlepredicates(const InputArray** relations,char* part,int relationsnum, int* relationIds, int queryIndex);
uint64_t** splitpreds(char* ch,int& cn);
// old function for predicate reordering used in the 2nd part of the project
uint64_t** optimizepredicates(uint64_t** preds,int cntr,int relationsnum,int* relationIds);
void predsplittoterms(char* pred,uint64_t& rel1,uint64_t& col1,uint64_t& rel2,uint64_t& col2,uint64_t& flag);
// creates and returns the hist constructed from the first "offset" elements of array
uint64_t* histcreate(tuple* array,int offset,int shift);
// creates and returns the psum constructed from the hist
uint64_t* psumcreate(uint64_t* hist);

// prints program's usage
void usage(char** argv);
// handles input arguments
void params(char** argv,int argc);
// calculates and prints the required sums by taking into consideration the parallelism mode of projection
void manageprojection(IntermediateArray* rowarr,const InputArray** array,char* part, int* relationIds,int queryIndex);
uint64_t** noopt(uint64_t** preds,int cntr);

#endif