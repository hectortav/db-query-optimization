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
        {5, 0xBFF}, {6, 0xAFF},
        {0, 0xBAA}, {1, 0xBAA}, {2, 0xAAA}, {3, 0xCAA}, {4, 0xCAA},
        {7, 0xCCC}, {8, 0xBCC}, {9, 0xACC}
                                        };
    std::cout << "before" << std::endl;
    R.print();
    relation *ro_R = re_ordered(&R, 0);
    // quickSort(R.tuples, 0, R.num_tuples - 1);
    std::cout << "\nafter" << std::endl;
    ro_R->print();

    delete ro_R;
}
