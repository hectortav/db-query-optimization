#include "utils.h"

/* The suite initialization function.
 * Opens the temporary file used by the tests.
 * Returns zero on success, non-zero otherwise.
 */
int init_suite(void)
{
//    if (NULL == (temp_file = fopen("temp.txt", "w+"))) {
//       return -1;
//    }
//    else {
//       return 0;
//    }
      return 0;
}

/* The suite cleanup function.
 * Closes the temporary file used by the tests.
 * Returns zero on success, non-zero otherwise.
 */
int clean_suite(void)
{
//    if (0 != fclose(temp_file)) {
//       return -1;
//    }
//    else {
//       temp_file = NULL;
//       return 0;
//    }
      return 0;
}

/* Simple test of fprintf().
 * Writes test data to the temporary file and checks
 * whether the expected number of bytes were written.
 */
void testSWAP(void)
{
      int i1 = randomIndex(1, 5);     
      CU_ASSERT(i1 <= 5 && i1 >= 1);
      // CU_ASSERT(2 == fprintf(temp_file, "Q\n"));
      // CU_ASSERT(7 == fprintf(temp_file, "i1 = %d", i1));
}