#include "functions.h"
#include <math.h>

void create_hist(relation *relR)
{
    int hist[pow(2, 8)][2] //static array?
    int64_t key;
    int i;
    for(i = 0; i < pow(2, 8); i++)
    {
        hist[i][0]= i;
        hist[i][1]= 0;
    }

    for (i = 0; i < relation.num_tuples; i++)
    {
        key = (relR.tuples[i].key >> 56) & 0xFF;
        hist[key][1]++;
    }
    

}