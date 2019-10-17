#include "functions.h"

//using namespace std;
//tuple is in std :/

int main(void)
{
    srand(time(NULL));
    relation R,S;
    
    R.num_tuples = 10;
    R.tuples = new tuple[R.num_tuples]/* {{0xF}, {0x1}, {0x1}, {0xF}, {0x3}, {0x3}, {0xA}, {0xA}, {0xC}, {0xC}};*/
    {
        {0xBFF,5}, {0xAFF,6},
        {0xBAA,0}, {0xBAA,1}, {0xAAA,2}, {0xCAA,3}, {0xCAA,4},
        {0xCCC,7}, {0xBCC,8}, {0xACC,9}
                                        };
    relation *ro_R = re_ordered(&R, 0);
    ro_R->print();

    delete ro_R;
}
