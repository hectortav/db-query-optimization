#include "functions.h"


int main(void)
{
    relation *new_rel_R = new relation(), *R = new relation();
    /*R->num_tuples = 4;
    R->tuples = new tuple[R->num_tuples]
    {
        {5, 0x00A}, {6, 0x0B1},
        {0, 0x01A}, {1, 0x0B0}
    };*/
    R->num_tuples = 10;
    R->tuples = new tuple[R->num_tuples]
    {
        {5, 0xBFF}, {6, 0xAFF},
        {0, 0xBAA}, {1, 0xBAA}, {2, 0xAAA}, {3, 0xCAA}, {4, 0xCAA},
        {7, 0xCCC}, {8, 0xBCC}, {9, 0xACC}
    };
    std::cout << "before" << std::endl;
    R->print();
    new_rel_R->num_tuples=R->num_tuples;
    new_rel_R->tuples = new tuple[R->num_tuples];
    new_rel_R = re_ordered(R, new_rel_R, 0);
    std::cout << "\nafter" << std::endl;
    new_rel_R->print();
    new_rel_R->~relation();
    R->~relation();
}

//g++ -g -o final main_static.cpp functions.cpp list.cpp && g++ -c -g main_static.cpp && g++ -c -g functions.cpp && g++ -c -g list.cpp && g++ -g -o final main_static.o functions.o list.o
