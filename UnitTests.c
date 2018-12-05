//
// Created by kats on 4/11/2018.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CUnit/Headers/Basic.h"
#include "CUnit/Headers/CUnit.h"
#include "DataGenerator.h"
#include "Index.h"


/* Pointer to the file used by the tests. */
static FILE* temp_file = NULL;

/* The suite initialization function.
 * Opens the temporary file used by the tests.
 * Returns zero on success, non-zero otherwise.
 */
int init_suite1(void)
{
    if (NULL == (temp_file = fopen("temp.txt", "w+"))) {
        return -1;
    }
    else {
        return 0;
    }
}


 /*The suite cleanup function.
 * Closes the temporary file used by the tests.
 * Returns zero on success, non-zero otherwise.*/


int clean_suite1(void)
{
    if (0 != fclose(temp_file)) {
        return -1;
    }
    else {
        temp_file = NULL;
        return 0;
    }
}


///////////////////////////////////////////////////////////////////////////
///////////////////////////  UNIT TESTS REGION  ///////////////////////////
///////////////////////////////////////////////////////////////////////////


/**
 * Test for the "power_of_two" function
 */
void testPOWEROFTWO(void){

    CU_ASSERT(32 == power_of_2(5));
    CU_ASSERT(1024 == power_of_2(10));

}


/**
 * Test for the "create_relation" function
 * Can't run because of multiple main methods both here and in DataGenerator.c
 */
/*void testCREATERELATION(void){

    relation* relR = create_relation_R();
    relation* relS = create_relation_S();

    int rsize = RELR_SIZE;
    int ssize = RELS_SIZE;

    CU_ASSERT(rsize == relR->num_tuples);
    CU_ASSERT(ssize == relS->num_tuples);
    CU_ASSERT(NULL != relR->tuples);
    CU_ASSERT(NULL != relS->tuples);

}*/


/**
 * Test for the "get_relation" function
 */
void testGETRELATION(void){

    int rsize = RELR_SIZE;
    int ssize = RELS_SIZE;

    relation* relR = get_relation((char*) relR_name, rsize);
    relation* relS = get_relation((char*) relS_name, ssize);

    CU_ASSERT(rsize == relR->num_tuples);
    CU_ASSERT(ssize == relS->num_tuples);
    CU_ASSERT(NULL != relR->tuples);
    CU_ASSERT(NULL != relS->tuples);

}


/**
 * Test for the "create_histogram" function
 * Takes for granted that tha radix suffix is 2
 */
void testCREATEHISTOGRAM(void){

    int rel_size = 10;

    relation* rel = malloc(sizeof(relation));
    rel->num_tuples = (uint32_t) rel_size;
    rel->tuples = malloc(rel_size * sizeof(tuple));

    for(int i=0; i<rel_size; i++){
        rel->tuples[i].key = i;
    }

    rel->tuples[0].payload = 3;
    rel->tuples[1].payload = 2;
    rel->tuples[2].payload = 5;
    rel->tuples[3].payload = 6;
    rel->tuples[4].payload = 3;
    rel->tuples[5].payload = 9;
    rel->tuples[6].payload = 8;
    rel->tuples[7].payload = 0;
    rel->tuples[8].payload = 3;
    rel->tuples[9].payload = 5;

    int* histogram = create_histogram(rel);

    CU_ASSERT(histogram[0] == 2);
    CU_ASSERT(histogram[1] == 3);
    CU_ASSERT(histogram[2] == 2);
    CU_ASSERT(histogram[3] == 3);
}



/**
 * Test for the "create_psum" function
 * Takes for granted that tha radix suffix is 2
 */
void testCREATEPSUM(void){

    int rel_size = 10;

    int histogram[] = {2, 3, 2, 3};
    int histogram1[] = {5, 1, 1, 3};

    int* psum1 = create_psum(histogram, rel_size);
    int* psum2 = create_psum(histogram1, rel_size);

    CU_ASSERT(psum1[0] == 0);
    CU_ASSERT(psum1[1] == 2);
    CU_ASSERT(psum1[2] == 5);
    CU_ASSERT(psum1[3] == 7);
    CU_ASSERT(psum2[0] == 0);
    CU_ASSERT(psum2[1] == 5);
    CU_ASSERT(psum2[2] == 6);
    CU_ASSERT(psum2[3] == 7);
}


/**
 * Test for the "create_relation_new" function
 */
void testCREATERELATIONNEW(void){

    int rel_size = 10;

    relation* rel = malloc(sizeof(relation));
    rel->num_tuples = (uint32_t) rel_size;
    rel->tuples = malloc(rel_size * sizeof(tuple));

    for(int i=0; i<rel_size; i++){
        rel->tuples[i].key = i;
    }

    rel->tuples[0].payload = 3;
    rel->tuples[1].payload = 2;
    rel->tuples[2].payload = 5;
    rel->tuples[3].payload = 6;
    rel->tuples[4].payload = 3;
    rel->tuples[5].payload = 9;
    rel->tuples[6].payload = 8;
    rel->tuples[7].payload = 0;
    rel->tuples[8].payload = 3;
    rel->tuples[9].payload = 5;

    int* histogram = create_histogram(rel);
    int* psum = create_psum(histogram, rel_size);

    relation* relNew = create_relation_new(rel, psum, 4);

    CU_ASSERT(relNew->tuples[0].payload == 8);
    CU_ASSERT(relNew->tuples[1].payload == 0);
    CU_ASSERT(relNew->tuples[2].payload == 5);
    CU_ASSERT(relNew->tuples[3].payload == 9);
    CU_ASSERT(relNew->tuples[4].payload == 5);
    CU_ASSERT(relNew->tuples[5].payload == 2);
    CU_ASSERT(relNew->tuples[6].payload == 6);
    CU_ASSERT(relNew->tuples[7].payload == 3);
    CU_ASSERT(relNew->tuples[8].payload == 3);
    CU_ASSERT(relNew->tuples[9].payload == 3);

}


/**
 * Test for the "index_fill" function
 */
void testINDEXFILL(void)
{
    //int radix_n = 2;
    int hash2_range = 10;

    int rel_size = 10;

    relation* rel = malloc(sizeof(relation));
    rel->num_tuples = (uint32_t) rel_size;
    rel->tuples = malloc(rel_size * sizeof(tuple));

    for(int i=0; i<rel_size; i++){
        rel->tuples[i].key = i;
    }

    rel->tuples[0].payload = 3;
    rel->tuples[1].payload = 2;
    rel->tuples[2].payload = 5;
    rel->tuples[3].payload = 6;
    rel->tuples[4].payload = 3;
    rel->tuples[5].payload = 9;
    rel->tuples[6].payload = 8;
    rel->tuples[7].payload = 0;
    rel->tuples[8].payload = 3;
    rel->tuples[9].payload = 5;    

    int* histogram = create_histogram(rel);
    int* psum = create_psum(histogram, rel_size);

    relation* relNew = create_relation_new(rel, psum, 4);

    hash_index *indx = NULL;
    index_create(&indx, hash2_range);


    index_fill(indx, relNew, 3, 5);

    CU_ASSERT(indx->bucket_array[0] == -1);
    CU_ASSERT(indx->bucket_array[1] == 2);
    CU_ASSERT(indx->bucket_array[2] == 1);
    CU_ASSERT(indx->bucket_array[3] == -1);
    CU_ASSERT(indx->bucket_array[4] == -1);
    CU_ASSERT(indx->bucket_array[5] == -1);
    CU_ASSERT(indx->bucket_array[6] == -1);
    CU_ASSERT(indx->bucket_array[7] == -1);
    CU_ASSERT(indx->bucket_array[8] == -1);
    CU_ASSERT(indx->bucket_array[9] == -1);

    CU_ASSERT(indx->chain[0] == -1);
    CU_ASSERT(indx->chain[1] == -1);
    CU_ASSERT(indx->chain[2] == 0);


    index_fill(indx, relNew, 3, 10);

    CU_ASSERT(indx->bucket_array[0] == 2);
    CU_ASSERT(indx->bucket_array[1] == -1);
    CU_ASSERT(indx->bucket_array[2] == -1);
    CU_ASSERT(indx->bucket_array[3] == -1);
    CU_ASSERT(indx->bucket_array[4] == -1);
    CU_ASSERT(indx->bucket_array[5] == -1);
    CU_ASSERT(indx->bucket_array[6] == -1);
    CU_ASSERT(indx->bucket_array[7] == -1);
    CU_ASSERT(indx->bucket_array[8] == -1);
    CU_ASSERT(indx->bucket_array[9] == -1);

    CU_ASSERT(indx->chain[0] == -1);
    CU_ASSERT(indx->chain[1] == 0);
    CU_ASSERT(indx->chain[2] == 1);

    index_destroy(&indx);
}




////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////     MAIN TO RUN TESTS   //////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

int main() {

    CU_pSuite pSuite = NULL;

    if (CUE_SUCCESS != CU_initialize_registry())
        return CU_get_error();

    pSuite = CU_add_suite("Suite_1", init_suite1, clean_suite1);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if ((NULL == CU_add_test(pSuite, "test of power_of_two", testPOWEROFTWO)) ||
        (NULL == CU_add_test(pSuite, "test of get_relation", testGETRELATION)) ||
        (NULL == CU_add_test(pSuite, "test of create_psum", testCREATEPSUM)) ||
        (NULL == CU_add_test(pSuite, "test of create_relation_new", testCREATERELATIONNEW)) ||
        (NULL == CU_add_test(pSuite, "test of create_histogram", testCREATEHISTOGRAM)) ||
        (NULL == CU_add_test(pSuite, "test of index_fill", testINDEXFILL))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}