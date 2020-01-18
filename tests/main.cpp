#include "utils/utils.h"

/* The main() function for setting up and running the tests.
 * Returns a CUE_SUCCESS on successful running, another
 * CUnit error code on failure.
 */
int main()
{
   CU_pSuite pSuite = NULL;

   /* initialize the CUnit test registry */
   if (CUE_SUCCESS != CU_initialize_registry())
      return CU_get_error();

   /* add a suite to the registry */
   pSuite = CU_add_suite("Suite", init_suite, clean_suite);
   if (NULL == pSuite) {
      CU_cleanup_registry();
      return CU_get_error();
   }

   /* add the tests to the suite */
   if ((NULL == CU_add_test(pSuite, "test of randomIndex()", testRandomIndex))
     || (NULL == CU_add_test(pSuite, "test of swap()", testSwap))
     || (NULL == CU_add_test(pSuite, "test of hashFunction()", testHashFunction))
     || (NULL == CU_add_test(pSuite, "test of makeparts()",testmakeparts))
     || (NULL == CU_add_test(pSuite, "test of splitpreds()",testsplitpreds))
     //|| (NULL == CU_add_test(pSuite, "test of optimizepredicates()",testoptimizepredicates))
     || (NULL == CU_add_test(pSuite, "test of predsplittoterms()",testpredsplittoterms))
     || (NULL == CU_add_test(pSuite, "test of sortBucket()", testQuickSort))
     || (NULL == CU_add_test(pSuite, "test of histcreateTest()", histcreateTest))
     || (NULL == CU_add_test(pSuite, "test of psumcreateTest()", psumcreateTest))
     || (NULL == CU_add_test(pSuite, "test of tuplesReorderTest()", tuplesReorderTest)) 
     || (NULL == CU_add_test(pSuite, "test of InputArray::filterRowIds()", testInputArrayFilterRowIds)) 
     || (NULL == CU_add_test(pSuite, "test of InputArray::extractColumnFromRowIds()", testInputArrayExtractColumnFromRowIds)) 
     || (NULL == CU_add_test(pSuite, "test of IntermediateArray::populate()", testIntermediateArrayPopulate)) 
     || (NULL == CU_add_test(pSuite, "test of IntermediateArray::findColumnIndexByInputArrayId() and IntermediateArray::findColumnIndexByPredicateArrayId()", testIntermediateArrayFindColumnIndex)) 
     || (NULL == CU_add_test(pSuite, "test of IntermediateArray::selfJoin()", testIntermediateArraySelfJoin))
     || (NULL == CU_add_test(pSuite, "test of getCombinationsNum()", testGetCombinationsNum))
     || (NULL == CU_add_test(pSuite, "test of getCombinations()", testGetCombinations))
     || (NULL == CU_add_test(pSuite, "test of Map",testmap))
    )
   {
      CU_cleanup_registry();
      return CU_get_error();
   }

   /* Run all tests using the CUnit Basic interface */
   CU_basic_set_mode(CU_BRM_VERBOSE);
   CU_basic_run_tests();
   CU_cleanup_registry();
   return CU_get_error();
}