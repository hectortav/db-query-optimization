#include "functions.h"


int main(void)
{
    relation *new_rel_R = new relation(), R;
    R.num_tuples = 10;
    R.tuples = new tuple[R.num_tuples]
    {
        {5, 0xBFF}, {6, 0xAFF},
        {0, 0xBAA}, {1, 0xBAA}, {2, 0xAAA}, {3, 0xCAA}, {4, 0xCAA},
        {7, 0xCCC}, {8, 0xBCC}, {9, 0xACC}
    };
    std::cout << "before" << std::endl;
    R.print();
    new_rel_R->num_tuples=R.num_tuples;
    new_rel_R->tuples = new tuple[R.num_tuples];
    new_rel_R = re_ordered_2(&R, new_rel_R);
    std::cout << "\nafter" << std::endl;
    new_rel_R->print();
}
