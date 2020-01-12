#ifndef _BESTTREEMAP_H
#define _BESTTREEMAP_H
#include "functions.h"

class Statistics
{
public:
    uint64_t min;
    uint64_t max;
    uint64_t numdiscrete;
    uint64_t size;

    Statistics(int min,int max,int numdiscrete,int size);
    ~Statistics();
};

class Predicate {
    public:
        int predicateArray1Id, predicateArray2Id;
        int field1Id, field2Id;

        void init(int predicateArray1Id, int field1Id, int predicateArray2Id, int field2Id);
        bool operator ==(Predicate &predicate);
        bool hasCommonArray(Predicate &predicate);
        void print(bool printEndl);
};

class PredicateArray
{
public:
    Predicate* array;
    int size;
    PredicateArray();
    PredicateArray(int size);
    ~PredicateArray();
    void init(PredicateArray* predicateArray, int size);
    bool contains(Predicate predicate);
    bool isConnectedWith(Predicate& predicate);
    void populate(PredicateArray *newArray);
    void print();
    bool operator ==(PredicateArray& array);
};

class Key{
public:
    PredicateArray* KeyArray;
    Key() {};
    Key(int);
    ~Key();
};

class ColumnEnumStats;
class ColumnStats;

class Value
{
public:
    PredicateArray* ValueArray;
    Statistics* stats;
    ColumnStats** columnStatsArray; // size: relationsnum x each InputArray's columnNum
    uint64_t cost;

    Value() {};
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
    // bool insert(int* key,int keysize,int* value,int valuesize);
    // Value* retrieve(int *key ,int size);
    // int exists(int * key,int keysize);
    void print();
};

class InputArray;

uint64_t** BestPredicateOrder(uint64_t** currentpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats );
uint64_t** OptimizePredicates(uint64_t**,int,int,int*,const InputArray**);
void FilterStats(uint64_t** filterpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats);



#endif