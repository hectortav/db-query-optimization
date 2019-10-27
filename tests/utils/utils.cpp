#include "utils.h"

int init_suite(void) {
    return 0;
}

int clean_suite(void) {
    return 0;
}

void testRandomIndex(void) {
    srand(time(NULL));
    for (int i = 0; i < 25; i++) {
        int index = randomIndex(10465, 200000);
        CU_ASSERT(index <= 200000 && index >= 10465)
        index = randomIndex(0, 1);
        CU_ASSERT(index <= 1 && index >= 0)
        index = randomIndex(0, 0);
        CU_ASSERT(index == 0)
        index = randomIndex(10, 10000);
        CU_ASSERT(index <= 10000 && index >= 10)
    }
}

void testSwap() {
    tuple* tuple1 = new tuple();
    tuple* tuple2 = new tuple();

    tuple1->key = 1000000000000;
    tuple1->payload = 0;
    tuple2->key = 9999999999999;
    tuple2->payload = 5000000000000;
    swap(tuple1, tuple2);
    CU_ASSERT(tuple1->key == 9999999999999 && tuple1->payload == 5000000000000 && tuple2->key == 1000000000000 && tuple2->payload == 0)

    tuple1->key = 0;
    tuple1->payload = 0;
    tuple2->key = 0;
    tuple2->payload = 0;
    swap(tuple1, tuple2);
    CU_ASSERT(tuple1->key == 0 && tuple1->payload == 0 && tuple2->key == 0 && tuple2->payload == 0)

    tuple1->key = 1;
    tuple1->payload = 5;
    tuple2->key = 6;
    tuple2->payload = 1;
    swap(tuple1, tuple2);
    CU_ASSERT(tuple1->key == 6 && tuple1->payload == 1 && tuple2->key == 1 && tuple2->payload == 5)
}

void testHashFunction() {
    CU_ASSERT(hashFunction(0xFF, 0) == 0xFF)
    CU_ASSERT(hashFunction(0xABCDEFAA, 3) == 0xAB)
    CU_ASSERT(hashFunction(0xABCDEFAA, 1) == 0xEF)
    CU_ASSERT(hashFunction(0x0, 3) == 0x0)
    CU_ASSERT(hashFunction(0xBBCC, 0) == 0xCC)
    CU_ASSERT(hashFunction(0xABC, 3) == 0x0)
    CU_ASSERT(hashFunction(0xABCEFBA, 2) == 0xBC)
    CU_ASSERT(hashFunction(0xABCEFBABCDEFCADD, 6) == 0xCE)
    CU_ASSERT(hashFunction(0xABCEFBABCDEFCADD, 7) == 0xAB)
    CU_ASSERT(hashFunction(0xABCEFBABCDEFCADD, 0) == 0xDD)
}