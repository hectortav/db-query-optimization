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
        {5, 0xB005}, {6, 0xB006},
        {0, 0xA000}, {1, 0xA001}, {2, 0xA002}, {3, 0xA003}, {4, 0xA004},
        {7, 0xB007}, {8, 0xB008}, {9, 0xB009}
    };
    std::cout << "before" << std::endl;
    R->print();
    new_rel_R->num_tuples=R->num_tuples;
    new_rel_R->tuples = new tuple[R->num_tuples];
    new_rel_R = re_ordered_2(R, new_rel_R, 0);
    std::cout << "\nafter" << std::endl;
    new_rel_R->print();
    new_rel_R->~relation();
    R->~relation();
}

//g++ -g -o final main_static.cpp functions.cpp list.cpp && g++ -c -g main_static.cpp && g++ -c -g functions.cpp && g++ -c -g list.cpp && g++ -g -o final main_static.o functions.o list.o
