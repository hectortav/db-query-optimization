#include <stdio.h>
#include <string.h>
#include <CUnit/Basic.h>
#include "../../functions.h"
#include "../../besttreemap.h"

/* The suite initialization function.
 * Returns zero on success, non-zero otherwise.
 */
int init_suite(void);

/* The suite cleanup function.
 * Returns zero on success, non-zero otherwise.
 */
int clean_suite(void);

/* Test of randomIndex().
 * Checks if the returned result of randomIndex() is between the start and end index given as arguments.
 */
void testRandomIndex(void);

/* Test of swap().
 * Checks if the keys and payloads of the 2 tuples given as arguments to swap() are swapped correctly.
 */
void testSwap(void);

/* Test of hashFunction().
 * Checks if the payload (uint64_t) given as argument to testHashFunction() is hashed correctly.
 */
void testHashFunction(void);

void testmakeparts(void);
void testsplitpreds(void);
void testoptimizepredicates(void);
void testpredsplittoterms(void);
void populateRelationRandomly(relation &rel);
bool isRelationOrdered(relation &rel);
void testQuickSort(void);
void histogramTest(void);
void psumTest(void);
void find_shiftTest(void);
void histcreateTest(void);
void psumcreateTest(void);
void tuplesReorderTest(void);
void re_orderedTest(void);
void testInputArrayFilterRowIds(void);
void testInputArrayExtractColumnFromRowIds(void);
void testIntermediateArrayPopulate(void);
void testIntermediateArrayFindColumnIndex(void);
void testIntermediateArraySelfJoin(void);
void testGetCombinationsNum(void);
void testGetCombinations(void);