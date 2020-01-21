#ifndef _BESTTREEMAP_H
#define _BESTTREEMAP_H
#include "functions.h"

class Predicate {
    public:
        int predicateArray1Id, predicateArray2Id;
        uint64_t field1Id, field2Id;

        void init(int predicateArray1Id, uint64_t field1Id, int predicateArray2Id, uint64_t field2Id);
        bool operator ==(Predicate &predicate);
        bool hasCommonArray(Predicate &predicate);
        void print(bool printEndl);
        bool issame(Predicate& prdct);

};

class PredicateArray
{
public:
    Predicate* array;
    int size;
    PredicateArray();
    PredicateArray(int size);
    PredicateArray(int size, uint64_t** preds);
    ~PredicateArray();
    void init(PredicateArray* predicateArray, int size);
    bool contains(Predicate predicate);
    bool isConnectedWith(Predicate& predicate);
    void populate(PredicateArray *newArray);
    void print();
    bool operator ==(PredicateArray& array);
    uint64_t** toUintArray();
};

class Key{
public:
    PredicateArray* KeyArray;
    Key() {this->KeyArray = NULL;};
    Key(int);
    ~Key();
};

class ColumnStats;

class Value
{
public:
    PredicateArray* ValueArray;
    ColumnStats** columnStatsArray; // size: relationsnum x each InputArray's columnNum
    uint64_t cost;
    int ColumnStatsArraySize;

    Value() {cost = 0;};
    Value(int);
    ~Value();
    
};

class Map
{
public:
    int cursize;
    Value** values;
    Key** keys;
    Map(int queryArraysNum);
    ~Map();
    bool insert(PredicateArray* key,Value* value);
    Value* retrieve(PredicateArray* key);
    int exists(PredicateArray* key);
    void print();
};

class InputArray;

// performs a Join Enumeration algorithm to find the best execution plan for the currently handled query
uint64_t** BestPredicateOrder(uint64_t** currentpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats );
// returns the correctly reordered predicates according to Join Enumeration (calls FilterStats() and BestPredicateOrder())
uint64_t** OptimizePredicates(uint64_t**,int&,int,int*,const InputArray**);
// calculates the ColumnStats of the arrays that take part in filters
void FilterStats(uint64_t** filterpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats);
// returns the number of all the current combinations of size combinationSize
int getCombinationsNum(int size, int combinationSize);
// constructs all the possible combinations (PredicateArray objects) of size r and stores them in resultArray
void getCombinations(PredicateArray* elements, int n, int r, int index, PredicateArray* data,
                     PredicateArray* resultArray, int i, int& nextIndex);  
// updates the ColumnStats for the array with id predicateArrayId for all the columns except the one with index joinFieldId
void updateColumnStats(const InputArray* pureInputArray, uint64_t joinFieldId, int predicateArrayId,
                         Value* valueP, Value* newValueP, ColumnStats** filterColumnStatsArray,
                         uint64_t fieldDistinctValuesNumAfterFilter, uint64_t fieldValuesNumBeforeFilter);           
// calculates and updates the ColumnStats of newValueP (the new join chain which includes newPredicateP) by using the ColumnStats of valueP (the old join chain)
void updateValueStats(Value* valueP, Value* newValueP, Predicate* newPredicateP, int* relationIds,
                         ColumnStats** filterColumnStatsArray, const InputArray** inputArrays);
// constructs and returns a new Value (new join chain) which contains newPredicateArrayP
Value* createJoinTree(Value* valueP, PredicateArray* newPredicateArrayP, int* relationIds, int relationsnum,
                         ColumnStats** filterColumnStatsArray, const InputArray** inputArrays);

#endif