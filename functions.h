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

const unsigned long BUCKET_SIZE = 64 * pow(2, 20);  //64KB (I think)
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

};

result* join(relation* R, relation* S);
int64_t** create_hist(relation*, int);
int64_t** create_psum(int64_t**);
relation* re_ordered(relation*, int);

#endif