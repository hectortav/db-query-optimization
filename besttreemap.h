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

class PredicateOperand {
    public:
        int predicateArrayId;
        int fieldId;
};

class PredicateOperandArray
{
public:
    PredicateOperand* array;
    int size;
    PredicateOperandArray();
    PredicateOperandArray(int size);
    ~PredicateOperandArray();
    void init(PredicateOperandArray* operandArray, int size);
    bool contains(PredicateOperand operand);
    void populate(PredicateOperandArray *newArray);
};

class Key{
public:
    PredicateOperandArray* KeyArray;
    Key(int*,int);
    ~Key();
};

class Value
{
public:
    PredicateOperandArray* ValueArray;
    Statistics* stats;
    Value(int* ,int);
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
    // bool insert(int* key,int keysize,int* value,int valuesize);
    // Value* retrieve(int *key ,int size);
    // int exists(int * key,int keysize);
    void print();
};

class InputArray;

uint64_t** BestPredicateOrder(uint64_t**,int,int,int*,InputArray** );


#endif