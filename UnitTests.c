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
#include "Actions.h"


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


///////////////////////////////////////////////////////////////////////////////////////
//////////////////////////      TABLE / RELATION TESTS      ///////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

/**
 * Test for the 'loadRelation' function.
 */
void testLOADRELATION(void){

    table* t = loadRelation("workloads/small/r0");

    CU_ASSERT_EQUAL(t->size, 1561);
    CU_ASSERT_EQUAL(t->columns[0][100], 301);
    CU_ASSERT_EQUAL(t->columns[2][456], 2781);

    free(t->metadata);
    free(t->columns);
    free(t);

}

/**
 * Test for the 'createTablesArray' function. Input has at least r0 in the first line and r1 in the second.
 */
void testCREATETABLESARRAY(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();

    CU_ASSERT_EQUAL(t[0]->size, 1561);
    CU_ASSERT_EQUAL(t[1]->size, 3754);
    CU_ASSERT_EQUAL(t[0]->columns[0][100], 301);
    CU_ASSERT_EQUAL(t[0]->columns[2][456], 2781);

    freeTable(t);

}

/**
 * Test for the 'parseTableData' function.
 */
void testPARSETABLEDATA(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    parseTableData(t);

    CU_ASSERT_EQUAL(t[0]->metadata[0].max, 4690);
    CU_ASSERT_EQUAL(t[0]->metadata[0].min, 1);
    CU_ASSERT_EQUAL(t[0]->metadata[0].distincts, 1561);

}

/**
 * Test for the 'getQueryFromLine' function.
 */
void testGETQUERYFROMLINE(void){

    char* s = malloc((strlen("5 1|0.1=1.0&0.2=4531|1.2")+10)*sizeof(char));
    strcpy(s, "5 1|0.1=1.0&0.2=4531|1.2");

    Query q = getQueryFromLine(s, strlen(s)+5);

    CU_ASSERT_EQUAL(q.column_set->columns_num, 1);
    CU_ASSERT_EQUAL(q.column_set->columns[0].relation, 1);
    CU_ASSERT_EQUAL(q.column_set->columns[0].column, 2);
    CU_ASSERT_EQUAL(q.comparison_set->comparisons_num, 2);
    CU_ASSERT_EQUAL(q.comparison_set->comparisons[0].relationA, 0);
    CU_ASSERT_EQUAL(q.comparison_set->comparisons[1].columnA, 2);
    CU_ASSERT_EQUAL(q.query_relation_set->query_relations_num, 2);
    CU_ASSERT_EQUAL(q.query_relation_set->query_relations[1], 1);
}

/**
 * Test for the 'getQueryRelations' function.
 */
void testGETQUERYRELATIONS(void){

    char* s = malloc((strlen("1 2 3 4")+1)*sizeof(char));
    strcpy(s, "1 2 3 4");

    Query_Relation_t* q = getQueryRelations(s);

    CU_ASSERT_EQUAL(q->query_relations_num, 4);
    CU_ASSERT_EQUAL(q->query_relations[0], 1);
    CU_ASSERT_EQUAL(q->query_relations[1], 2);
    CU_ASSERT_EQUAL(q->query_relations[2], 3);
    CU_ASSERT_EQUAL(q->query_relations[3], 4);

    free(q->query_relations);
}

/**
 * Test for the 'getComparisons' function.
 */
void testGETCOMPARISONS(void){

    char* s = malloc((strlen("1.1=0.2&1.3<400")+1)*sizeof(char));
    strcpy(s, "1.1=0.2&1.3<400");

    Comparison_t* q = getComparisons(s);

    CU_ASSERT_EQUAL(q->comparisons_num, 2);
    CU_ASSERT_EQUAL(q->comparisons[0].columnA, 1);
    CU_ASSERT_EQUAL(q->comparisons[1].relationB, 400);
    CU_ASSERT_EQUAL(q->comparisons[0].action, 0);

}

/**
 * Test for the 'getColumns' function.
 */
void testGETCOLUMNS(void){

    char* s = malloc((strlen("1.1 0.2")+1)*sizeof(char));
    strcpy(s, "1.1 0.2");

    Column_t* q = getColumns(s);

    CU_ASSERT_EQUAL(q->columns_num, 2);
    CU_ASSERT_EQUAL(q->columns[0].column, 1);
    CU_ASSERT_EQUAL(q->columns[0].relation, 1);
    CU_ASSERT_EQUAL(q->columns[1].column, 2);
    CU_ASSERT_EQUAL(q->columns[1].relation, 0);

    free(q);

}

/**
 * Test for the 'getComparisonFromQuery' function.
 */
void testGETCOMPARISONFROMQUERY(void){

    char* s = malloc((strlen("1.1=0.2")+1)*sizeof(char));
    strcpy(s, "1.1=0.2");

    Comparison q = getComparisonFromQuery(s);

    CU_ASSERT_EQUAL(q.action, 0);
    CU_ASSERT_EQUAL(q.relationA, 1);
    CU_ASSERT_EQUAL(q.relationB, 0);
    CU_ASSERT_EQUAL(q.columnA, 1);
    CU_ASSERT_EQUAL(q.columnB, 2);

    free(s);

}

/**
 * Test for the 'getColumnFromQuery' function.
 */
void testGETCOLUMNFROMQUERY(void){

    char* s = malloc((strlen("1.2")+1)*sizeof(char));
    strcpy(s, "1.2");

    Column q = getColumnsFromQuery(s);

    CU_ASSERT_EQUAL(q.relation, 1);
    CU_ASSERT_EQUAL(q.column, 2);

    free(s);

}


///////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////      ACTIONS TESTS      ////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

/**
 * Test for the 'createList' function.
 */
void testCREATELIST(void){

    IntermediateResultsList* l = createList();

    CU_ASSERT_EQUAL(l->next, NULL);
    CU_ASSERT_EQUAL(l->table->relAmount, 0);

    free(l->table);
    free(l);

}

/**
 * Test for the 'compareColumns' function.
 */
void testCOMPARECOLUMNS(void){

    freopen("input", "r", stdin);

    /*table** t = createTablesArray();
    IntermediateResultsList* l = createList();

    l = compareColumn(l, t[1], 0, 0, 11400, GREATER_THAN);

    CU_ASSERT_EQUAL(l->next->table->tupleAmount, 3);
    CU_ASSERT_EQUAL(l->next->table->relAmount, 1);

    freeTable(t);
    IntermediateResultsDel(l->table);
    free(l);*/

}

/**
 * Test for the 'joinSameRelation' function.
 */
void testJOINSAMERELATION(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResultsList* l = createList();
    int* arr = malloc(2*sizeof(int));
    arr[0] = 1;
    arr[1] = 1;


    l = joinSameRelation(l, t, arr, 0, 0, 1);

    CU_ASSERT_EQUAL(l->next->table->tupleAmount, 1);
    CU_ASSERT_EQUAL(l->next->table->keys[0][0], 1015);

    freeTable(t);
    free(l);

}

/**
 * Test for the 'joinRelationsRadix' function.
 */
void testJOINRELATIONSRADIX(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResultsList* l = createList();

    int* arr = malloc(2*sizeof(int));
    arr[0] = 1;
    arr[1] = 2;

    l = joinRelationsRadix(l, t, arr, 0, 1, 0, 1);

    CU_ASSERT_EQUAL(l->next->table->tupleAmount, 26808);
    CU_ASSERT_EQUAL(l->next->table->relationIDs[0], 0);
    CU_ASSERT_EQUAL(l->next->table->relationIDs[1], 1);

    freeTable(t);
    free(l);

}

/**
 * Test for the 'crossProductIntermediateResults' function.
 */
void testCROSSPRODUCTINTERMEDIATERESULTS(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResults* r1 = createIntermediateResultFromTable(t[0], 0);
    IntermediateResults* r2 = createIntermediateResultFromTable(t[1], 1);

    IntermediateResults* l = crossProductIntermediateResults(r1, r2);

    CU_ASSERT_EQUAL(l->tupleAmount, r1->tupleAmount*r2->tupleAmount);
    CU_ASSERT_EQUAL(l->relAmount, 2);
    CU_ASSERT_EQUAL(l->relationIDs[0], 0);
    CU_ASSERT_EQUAL(l->relationIDs[1], 1);
    CU_ASSERT_EQUAL(l->keys[0][0], 0);
    CU_ASSERT_EQUAL(l->keys[1][0], 0);

    freeTable(t);

}

/**
 * Test for the 'createRelationFromTable' function.
 */
void testCREATERELATIONFROMTABLE(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    relation* r = createRelationFromTable(t[0], 1);

    CU_ASSERT_EQUAL(r->num_tuples, 1561);
    CU_ASSERT_EQUAL(r->tuples[2].payload, 8807);
    CU_ASSERT_EQUAL(r->tuples[204].payload, 4408);

    freeTable(t);
    freeRelation(r);

}

/**
 * Test for the 'createRelationFromIntermediateResults' function.
 */
void testCREATERELATIONFROMINTERMEDIATERESULTS(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResults* r1 = createIntermediateResultFromTable(t[0], 0);
    relation* r = createRelationFromIntermediateResults(r1, t[0], 0, 1);

    CU_ASSERT_EQUAL(r->num_tuples, 1561);
    CU_ASSERT_EQUAL(r->tuples[2].payload, 8807);
    CU_ASSERT_EQUAL(r->tuples[204].payload, 4408);

    freeTable(t);
    freeRelation(r);

}

/**
 * Test for the 'calculateActionResultAmount' function.
 */
void testCALCULATEACTIONRESULTAMOUNT(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    relation* r = createRelationFromTable(t[0], 0);

    int amount = calculateActionResultAmount(r, 4600, 2);

    CU_ASSERT_EQUAL(amount, 31);

    freeTable(t);
    freeRelation(r);

}

/**
 * Test for the 'comparePayloadToValue' function.
 */
void testCOMPAREPAYLOADTOVALUE(void){

    int a = comparePayloadToValue(234, 2, 2);
    int b = comparePayloadToValue(234, 239, 2);
    int c = comparePayloadToValue(234, 234, 3);

    CU_ASSERT_EQUAL(a, 1);
    CU_ASSERT_EQUAL(b, 0);
    CU_ASSERT_EQUAL(c, 1);

}

/**
 * Test for the 'existsInIntermediateResults' function.
 */
void testEXISTSININTERMEDIATERESULTS(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResults* r = createIntermediateResultFromTable(t[0], 0);

    int flag = existsInIntermediateResults(r, 0);
    int flag1 = existsInIntermediateResults(r, 2);

    CU_ASSERT_EQUAL(flag, 1);
    CU_ASSERT_EQUAL(flag1, 0);

    freeTable(t);

}

/**
 * Test for the 'getIntermediateResultsIndex' function.
 */
void testGETINTERMEDIATERESULTSINDEX(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResultsList* inRes = createList();
    IntermediateResults* r = createIntermediateResultFromTable(t[0], 0);
    addNodeToList(inRes, r);

    int r1 = getIntermediateResultsIndex(inRes, 0, 0);
    int r2 = getIntermediateResultsIndex(inRes, 0, 1);
    int r3 = getIntermediateResultsIndex(inRes, 3, 1);

    CU_ASSERT_EQUAL(r1, 0);
    CU_ASSERT_EQUAL(r2, 0);
    CU_ASSERT_EQUAL(r3, -1);

    freeTable(t);
    free(inRes);
}

/**
 * Test for the 'getIntermediateResultsSingleIndex' function.
 */
void testGETINTERMEDIATERESULTSSINGLEINDEX(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResultsList* inRes = createList();
    IntermediateResults* r = createIntermediateResultFromTable(t[0], 0);
    addNodeToList(inRes, r);

    int r1 = getIntermediateResultsSingleIndex(inRes, 0);
    int r2 = getIntermediateResultsSingleIndex(inRes, 1);

    CU_ASSERT_EQUAL(r1, 0);
    CU_ASSERT_EQUAL(r2, -1);

    freeTable(t);
    free(inRes);
}

/**
 * Test for the 'getQueryCategory' function.
 */
void testGETQUERYCATEGORY(void){

    freopen("input", "r", stdin);

    table** t = createTablesArray();
    IntermediateResultsList* inRes = createList();
    IntermediateResults* r = createIntermediateResultFromTable(t[0], 0);
    addNodeToList(inRes, r);

    int r1 = getQueryCategory(inRes, 0, 1);
    int r2 = getQueryCategory(inRes, 0, 0);
    int r3 = getQueryCategory(inRes, 2, 1);

    CU_ASSERT_EQUAL(r1, 1);
    CU_ASSERT_EQUAL(r2, 2);
    CU_ASSERT_EQUAL(r3, 0);

    freeTable(t);
    free(inRes);
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

    if ((NULL == CU_add_test(pSuite, "test of loadRelation", testLOADRELATION)) ||
        (NULL == CU_add_test(pSuite, "test of createTablesArray", testCREATETABLESARRAY)) ||
        (NULL == CU_add_test(pSuite, "test of createList", testCREATELIST)) ||
        (NULL == CU_add_test(pSuite, "test of compareColumns", testCOMPARECOLUMNS)) ||
        (NULL == CU_add_test(pSuite, "test of getQueryRelations", testGETQUERYRELATIONS)) ||
        (NULL == CU_add_test(pSuite, "test of getComparisons", testGETCOMPARISONS)) ||
        (NULL == CU_add_test(pSuite, "test of getColumns", testGETCOLUMNS)) ||
        (NULL == CU_add_test(pSuite, "test of getComparisonFromQuery", testGETCOMPARISONFROMQUERY)) ||
        (NULL == CU_add_test(pSuite, "test of getColumnFromQuery", testGETCOLUMNFROMQUERY)) ||
        (NULL == CU_add_test(pSuite, "test of joinSameRelation", testJOINSAMERELATION)) ||
        (NULL == CU_add_test(pSuite, "test of joinRelationsRadix", testJOINRELATIONSRADIX)) ||
        (NULL == CU_add_test(pSuite, "test of crossProductIntermediateResults", testCROSSPRODUCTINTERMEDIATERESULTS)) ||
        (NULL == CU_add_test(pSuite, "test of createRelationFromTable", testCREATERELATIONFROMTABLE)) ||
        (NULL == CU_add_test(pSuite, "test of createRelationFromIntermediateResults", testCREATERELATIONFROMINTERMEDIATERESULTS)) ||
        (NULL == CU_add_test(pSuite, "test of calculateActionResultAmount", testCALCULATEACTIONRESULTAMOUNT)) ||
        (NULL == CU_add_test(pSuite, "test of comparePayloadToValue", testCOMPAREPAYLOADTOVALUE)) ||
        (NULL == CU_add_test(pSuite, "test of existsInIntermediateResults", testEXISTSININTERMEDIATERESULTS)) ||
        (NULL == CU_add_test(pSuite, "test of getIntermediateResultsIndex", testGETINTERMEDIATERESULTSINDEX)) ||
        (NULL == CU_add_test(pSuite, "test of getIntermediateResultsSingleIndex", testGETINTERMEDIATERESULTSSINGLEINDEX)) ||
        (NULL == CU_add_test(pSuite, "test of getQueryCategory", testGETQUERYCATEGORY)) ||
        (NULL == CU_add_test(pSuite, "test of getQueryFromLine", testGETQUERYFROMLINE))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}