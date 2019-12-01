#ifndef _FUNCTIONS_H
#define _FUNCTIONS_H
#include <iostream>
#include <cstdlib>
#include <cstring>
#include "list.h"
#include <ctime>
#include <cstdio>
#include <math.h>

#define MAX_INPUT_FILE_NAME_SIZE 30
#define MAX_INPUT_ARRAYS_NUM 20

typedef class list list;


class tuple
{
public:
    uint64_t key;
    uint64_t payload;
};

const unsigned long BUCKET_SIZE = 64 * pow(2, 10);  //64KB (I think)
const unsigned long TUPLE_SIZE = sizeof(tuple);
const int TUPLES_PER_BUCKET = 2;//(int)(BUCKET_SIZE / TUPLE_SIZE);  
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

class InputArray
{
    public:
    uint64_t rowsNum, columnsNum;
    uint64_t** columns;

    InputArray(uint64_t rowsNum, uint64_t columnsNum);
    InputArray(uint64_t rowsNum);  // initialization for storing row ids
    ~InputArray();

    InputArray* filterRowIds(uint64_t fieldId, int operation, uint64_t numToCompare, InputArray* pureInputArray); // filtering when storing row ids
    void extractColumnFromRowIds(relation& rel, uint64_t fieldId, InputArray* pureInputArray); // column extraction from the initial input array (pureInputArray)
    void print();
};

class IntermediateArray {
    public:
    uint64_t** results; // contains columns of rowIds: one column per used input array
    int* inputArrayIds; // size = columnsNum
                        // contains ids of input arrays that correspond to each column
    uint64_t columnsNum;
    uint64_t rowsNum;
    uint64_t sortedByFieldId;
    int sortedByInputArrayId;

    IntermediateArray(uint64_t columnsNum, uint64_t sortedByInputArrayId, uint64_t sortedByFieldId);
    ~IntermediateArray();

    void extractFieldToRelation(relation* resultRelation, InputArray* inputArray, int inputArrayId, uint64_t fieldId);
    void populate(uint64_t** intermediateResult, uint64_t rowsNum, IntermediateArray* prevIntermediateArray, int inputArray1Id, int inputArray2Id);
    bool hasInputArrayId(int inputArrayId);
    bool shouldSort(int nextQueryInputArrayId, uint64_t nextQueryFieldId);
    void print();
};

class _vector
{
    public:
    uint64_t index, value;
    _vector *vptr;

    _vector();
    ~_vector();
};

unsigned char hashFunction(uint64_t payload, int shift);
result* join(relation* R, relation* S,uint64_t**r,uint64_t**s,int rsz,int ssz,int joincol);
uint64_t** create_hist(relation*, int);
uint64_t** create_psum(uint64_t**);
relation* re_ordered(relation*,relation*, int);
relation* re_ordered_2(relation*,relation*); //temporary

// functions for bucket sort
void swap(tuple* tuple1, tuple* tuple2);
int randomIndex(int startIndex, int stopIndex);
int partition(tuple* tuples, int startIndex, int stopIndex);
void quickSort(tuple* tuples, int startIndex, int stopIndex);
void sortBucket(relation* rel, int startIndex, int endIndex);
void extractcolumn(relation& rel,uint64_t **array, int column);
InputArray** readArrays();
char** readbatch(int& lns);
char** makeparts(char* query);
void handlequery(char** parts,InputArray** allrelations);
void loadrelationIds(int* relationIds, char* part, int& relationsnum);
// InputArray** loadrelations(char* part,InputArray** allrelations,int& relationsnum);
IntermediateArray* handlepredicates(InputArray** relations,char* part,int relationsnum, int* relationIds);
void handleprojection(IntermediateArray* rowarr,InputArray** array,char* part);
uint64_t** splitpreds(char* ch,int& cn);
uint64_t** optimizepredicates(uint64_t** preds,int cntr,int relationsnum);
void predsplittoterms(char* pred,uint64_t& rel1,uint64_t& col1,uint64_t& rel2,uint64_t& col2,uint64_t& flag);



#endif