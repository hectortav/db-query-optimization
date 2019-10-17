#include "functions.h"

//using namespace std;
//tuple is in std :/

int main(void)
{
    srand(time(NULL));
    relation R,S;
    
    R.num_tuples = 10;
    R.tuples = new tuple[R.num_tuples] {{0xF,0}, {0xF,1}, {0xF,2}, {0xF,3}, {0xF,4},
                                        {0xFF,5}, {0xFF,6},
                                        {0x0,7}, {0x0,8}, {0x0,9}};
    relation *ro_R = re_ordered(&R);
    ro_R->print();

    delete ro_R;
}
