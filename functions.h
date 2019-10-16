#ifndef _FUNCTIONS_H
#define _FUNCTIONS_H
#include <iostream>
#include <cstdlib>
#include <cstring>
#include "list.h"
#include <ctime>
#include <cstdio>
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

};

result* join(relation* R, relation* S);

#endif