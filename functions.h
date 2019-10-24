#ifndef _FUNCTIONS_H
#define _FUNCTIONS_H
#include <iostream>
#include <cstdlib>
#include <cstring>
#include "list.h"
#include <ctime>
#include <cstdio>
#include <math.h>

class tuple
{
public:
    int64_t key;
    int64_t payload;
};

class relation
{
public:
    tuple* tuples;
    uint64_t num_tuples;
    void print();
};

class result
{
public:
    list* lst;
};

result* join(relation* R, relation* S,int64_t**r,int64_t**s,int rsz,int ssz,int joincol);

#endif