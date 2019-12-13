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


void testmakeparts(void)
{
    char str[100];
    strcpy(str,"0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1");
    char** p=makeparts(str);
    CU_ASSERT(strcmp(p[0],"0 2 4")==0 && strcmp(p[1],"0.1=1.2&1.0=2.1&0.1>3000")==0  &&  strcmp(p[2],"0.0 1.1")==0);
    
    strcpy(str,"8 0 11|1.0=0.2&1.0=2.2&0.3=9477|0.2");
    p=makeparts(str);
    CU_ASSERT(strcmp(p[0],"8 0 11")==0 && strcmp(p[1],"1.0=0.2&1.0=2.2&0.3=9477")==0  &&  strcmp(p[2],"0.2")==0);

    strcpy(str,"6 1 11 5|0.1=1.0&1.0=2.1&1.0=3.1&0.0>44809|2.0");
    p=makeparts(str);
    CU_ASSERT(strcmp(p[0],"6 1 11 5")==0 && strcmp(p[1],"0.1=1.0&1.0=2.1&1.0=3.1&0.0>44809")==0  &&  strcmp(p[2],"2.0")==0);

    strcpy(str,"8 0 7|0.2=1.0&1.0=2.1&0.3>10502|1.1 1.2 2.5");
    p=makeparts(str);
    CU_ASSERT(strcmp(p[0],"8 0 7")==0 && strcmp(p[1],"0.2=1.0&1.0=2.1&0.3>10502")==0  &&  strcmp(p[2],"1.1 1.2 2.5")==0);

    strcpy(str,"12 1 6 12|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2");
    p=makeparts(str);
    CU_ASSERT(strcmp(p[0],"12 1 6 12")==0 && strcmp(p[1],"0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199")==0  &&  strcmp(p[2],"2.1 0.1 0.2")==0);
}

void testsplitpreds(void)
{
    uint64_t** p;
    int cn;
    char str[100];

    strcpy(str,"0.1=1.2&1.0=2.1&0.1>3000");
    p=splitpreds(str,cn);
    CU_ASSERT(p[0][0]==0 && p[0][1]==1 && p[0][2]==2 && p[0][3]==1 && p[0][4]==2 && p[1][0]==1 && p[1][1]==0 && p[1][2]==2 && p[1][3]==2 && p[1][4]==1 && p[2][0]==0 && p[2][1]==1 && p[2][2]==0 && p[2][3]==(uint64_t)-1 && p[2][4]==3000);

    strcpy(str,"1.0=0.2&1.0=2.2&0.3=9477");
    p=splitpreds(str,cn);
    CU_ASSERT(p[0][0]==1 && p[0][1]==0 && p[0][2]==2 && p[0][3]==0 && p[0][4]==2 && p[1][0]==1 && p[1][1]==0 && p[1][2]==2 && p[1][3]==2 && p[1][4]==2 && p[2][0]==0 && p[2][1]==3 && p[2][2]==2 && p[2][3]==(uint64_t)-1 && p[2][4]==9477);

    strcpy(str,"0.1=1.0&1.0=2.1&1.0=3.1&0.0>44809");
    p=splitpreds(str,cn);
    CU_ASSERT(p[0][0]==0 && p[0][1]==1 && p[0][2]==2 && p[0][3]==1 && p[0][4]==0 && p[1][0]==1 && p[1][1]==0 && p[1][2]==2 && p[1][3]==2 && p[1][4]==1 && p[2][0]==1 && p[2][1]==0 && p[2][2]==2 && p[2][3]==3 && p[2][4]==1 && p[3][0]==0 && p[3][1]==0 &&p[3][2]==0 && p[3][3]==(uint64_t)-1 && p[3][4]==44809);

    strcpy(str,"0.2=1.0&1.0=2.1&0.3>10502");
    p=splitpreds(str,cn);
    CU_ASSERT(p[0][0]==0 && p[0][1]==2 && p[0][2]==2 && p[0][3]==1 && p[0][4]==0 && p[1][0]==1 && p[1][1]==0 && p[1][2]==2 && p[1][3]==2 && p[1][4]==1 && p[2][0]==0 && p[2][1]==3 && p[2][2]==0 && p[2][3]==(uint64_t)-1 && p[2][4]==10502);

    strcpy(str,"0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199");
    p=splitpreds(str,cn);
    CU_ASSERT(p[0][0]==0 && p[0][1]==2 && p[0][2]==2 && p[0][3]==1 && p[0][4]==0 && p[1][0]==1 && p[1][1]==0 && p[1][2]==2 && p[1][3]==2 && p[1][4]==1 && p[2][0]==0 && p[2][1]==1 && p[2][2]==2 && p[2][3]==3 && p[2][4]==2 && p[3][0]==3 && p[3][1]==0 &&p[3][2]==1 && p[3][3]==(uint64_t)-1 && p[3][4]==33199);
}
/*void testoptimizepredicates(void)
{
    //uint64_t p[3][5]={{0,1,2,1,2},{1,0,2,2,1},{0,1,0,(uint64_t)-1,3000}};
    uint64_t** p;
    p= new uint64_t*[4];

    p[0]=new uint64_t[5]{0,1,2,1,2};
    p[1]=new uint64_t[5]{1,0,2,2,1};
    p[2]=new uint64_t[5]{0,1,0,(uint64_t)-1,3000};
    p=optimizepredicates(p,3,3);
    CU_ASSERT(p[1][0]==0 && p[1][1]==1 && p[1][2]==2 && p[1][3]==1 && p[1][4]==2 && p[2][0]==1 && p[2][1]==0 && p[2][2]==2 && p[2][3]==2 && p[2][4]==1 && p[0][0]==0 && p[0][1]==1 && p[0][2]==0 && p[0][3]==(uint64_t)-1 && p[0][4]==3000);
    
    p[0]=new uint64_t[5]{1,0,2,0,2};
    p[1]=new uint64_t[5]{1,0,2,2,2};
    p[2]=new uint64_t[5]{0,3,2,(uint64_t)-1,9477};
    p=optimizepredicates(p,3,3);
    CU_ASSERT(p[1][0]==1 && p[1][1]==0 && p[1][2]==2 && p[1][3]==0 && p[1][4]==2 && p[2][0]==1 && p[2][1]==0 && p[2][2]==2 && p[2][3]==2 && p[2][4]==2 && p[0][0]==0 && p[0][1]==3 && p[0][2]==2 && p[0][3]==(uint64_t)-1 && p[0][4]==9477);

    p[0]=new uint64_t[5]{0,1,2,1,0};
    p[1]=new uint64_t[5]{1,0,2,2,1};
    p[2]=new uint64_t[5]{1,0,2,3,1};
    p[3]=new uint64_t[5]{0,0,0,(uint64_t)-1,44809};
    p=optimizepredicates(p,4,4);
    CU_ASSERT(p[1][0]==0 && p[1][1]==1 && p[1][2]==2 && p[1][3]==1 && p[1][4]==0 && p[2][0]==1 && p[2][1]==0 && p[2][2]==2 && p[2][3]==2 && p[2][4]==1 && p[3][0]==1 && p[3][1]==0 && p[3][2]==2 && p[3][3]==3 && p[3][4]==1 && p[0][0]==0 && p[0][1]==0 &&p[0][2]==0 && p[0][3]==(uint64_t)-1 && p[0][4]==44809);
    

    p[0]=new uint64_t[5]{0,2,2,1,0};
    p[1]=new uint64_t[5]{1,0,2,2,1};
    p[2]=new uint64_t[5]{0,3,0,(uint64_t)-1,10502};
    p=optimizepredicates(p,3,3);
    CU_ASSERT(p[1][0]==0 && p[1][1]==2 && p[1][2]==2 && p[1][3]==1 && p[1][4]==0 && p[2][0]==1 && p[2][1]==0 && p[2][2]==2 && p[2][3]==2 && p[2][4]==1 && p[0][0]==0 && p[0][1]==3 && p[0][2]==0 && p[0][3]==(uint64_t)-1 && p[0][4]==10502);
    

    p[0]=new uint64_t[5]{0,2,2,1,0};
    p[1]=new uint64_t[5]{1,0,2,2,1};
    p[2]=new uint64_t[5]{0,1,2,3,2};
    p[3]=new uint64_t[5]{3,0,1,(uint64_t)-1,33199};
    p=optimizepredicates(p,4,4);
    CU_ASSERT(p[1][0]==0 && p[1][1]==2 && p[1][2]==2 && p[1][3]==1 && p[1][4]==0 && p[3][0]==1 && p[3][1]==0 && p[3][2]==2 && p[3][3]==2 && p[3][4]==1 && p[2][0]==0 && p[2][1]==1 && p[2][2]==2 && p[2][3]==3 && p[2][4]==2 && p[0][0]==3 && p[0][1]==0 &&p[0][2]==1 && p[0][3]==(uint64_t)-1 && p[0][4]==33199);
    
    
}*/

void testpredsplittoterms(void)
{
    uint64_t c1,c2,r1,r2,op;
    char str[100];
    
    strcpy(str,"0.1=1.2");
    predsplittoterms(str,r1,c1,r2,c2,op);
    CU_ASSERT(r1==0 && c1==1 && op==2 && r2==1 && c2==2);

    strcpy(str,"0.3>10502");
    predsplittoterms(str,r1,c1,r2,c2,op);
    CU_ASSERT(r1==0 && c1==3 && op==0 && r2==(uint64_t)-1 && c2==10502);

    strcpy(str,"0.0>44809");
    predsplittoterms(str,r1,c1,r2,c2,op);
    CU_ASSERT(r1==0 && c1==0 && op==0 && r2==(uint64_t)-1 && c2==44809);

    strcpy(str,"3.0<33199");
    predsplittoterms(str,r1,c1,r2,c2,op);
    CU_ASSERT(r1==3 && c1==0 && op==1 && r2==(uint64_t)-1 && c2==33199);

    strcpy(str,"1.0=2.2");
    predsplittoterms(str,r1,c1,r2,c2,op);
    CU_ASSERT(r1==1 && c1==0 && op==2 && r2==2 && c2==2);
}

void populateRelationRandomly(relation &rel) {
    rel.num_tuples = rand()%100000;
    rel.tuples = new tuple[rel.num_tuples];
    for (int i = 0; i < rel.num_tuples; i++) {
        rel.tuples[i].key = rand();
        rel.tuples[i].payload = rand();
    }
}

bool isRelationOrderedTest(relation &rel) {
    for (int i = 0; i < rel.num_tuples - 1; i++) {
        if (rel.tuples[i].payload > rel.tuples[i + 1].payload)
            return false;
    }

    return true;
}

void testQuickSort(void) {
    srand(time(NULL));

    relation rel1, rel2, rel3, rel4, rel5;
    populateRelationRandomly(rel1);
    populateRelationRandomly(rel2);
    populateRelationRandomly(rel3);
    populateRelationRandomly(rel4);
    populateRelationRandomly(rel5);

    quickSort(rel1.tuples, 0, rel1.num_tuples - 1);
    quickSort(rel2.tuples, 0, rel2.num_tuples - 1);
    quickSort(rel3.tuples, 0, rel3.num_tuples - 1);
    quickSort(rel4.tuples, 0, rel4.num_tuples - 1);
    quickSort(rel5.tuples, 0, rel5.num_tuples - 1);
    
    CU_ASSERT(isRelationOrderedTest(rel1) && isRelationOrderedTest(rel2) && isRelationOrderedTest(rel3) && isRelationOrderedTest(rel4) && isRelationOrderedTest(rel5));
}

void histogramTest(void) {
    relation rel;
    populateRelationRandomly(rel);
    uint64_t **hist = create_hist(&rel, 0);
    uint64_t **hist_2 = create_hist(&rel, 0);

    int x = pow(2,8);
    uint64_t item_count = 0;

    for (int i = 0; i < x; i++)
    {
        CU_ASSERT(hist[0][i] == hist_2[0][i]);
        CU_ASSERT(hist[1][i] == hist_2[1][i]);
        CU_ASSERT(hist[2][i] == hist_2[2][i]);
        item_count += hist[1][i];
    }
    CU_ASSERT(item_count == rel.num_tuples);
}