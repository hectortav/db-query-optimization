#include "functions.h"

//using namespace std;
//tuple is in std :/

int main(void)
{
    srand(time(NULL));
    relation R,S;

    R.num_tuples = 10;
    R.tuples = new tuple[R.num_tuples] {{0xF,0}, {0xFF,0}, {0xF,0}, {0,0}, {0,0}, {0,0}, {0xFF,0}, {0xFF,0}, {0xFF,0}, {0,0}};
    re_ordered(&R);

    delete [] R.tuples;
}
