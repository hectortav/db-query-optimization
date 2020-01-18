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

uint64_t** BestPredicateOrder(uint64_t** currentpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats );
uint64_t** OptimizePredicates(uint64_t**,int&,int,int*,const InputArray**);
void FilterStats(uint64_t** filterpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats);



#endif