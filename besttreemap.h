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

class Array
{
public:
    int* array;
    int size;
    Array();
    ~Array();
};

class Key{
public:
    Array* KeyArray;
    Key(int*,int);
    ~Key();
};

class Value
{
public:
    Array* ValueArray;
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
    bool insert(int* key,int keysize,int* value,int valuesize);
    Value* retrieve(int *key ,int size);
    int exists(int * key,int keysize);
    void print();
};






#endif