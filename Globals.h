#ifndef _GLOBALS_H_
#define _GLOBALS_H_

#include <stdint.h>

#define RELR_SIZE 100                             // Relation R size
#define RELS_SIZE 100                              // Relation S size
#define MAX_VALUE 1000                                  // Max value that the relation values can go to
#define RADIX_N 5
#define THREAD_NUM 12

#define R 0
#define S 1

static const char relR_name[] = "DataRelationR.txt";    // Text file containing all the values of the first relation
static const char relS_name[] = "DataRelationS.txt";    // Text file containing all the values of the second relation

/** Type definition for a tuple */
typedef struct tuple {
    int32_t key;
    int32_t payload;
} tuple;
/**
 * Type definition for a relation.
 * It consists of an array of tuples and a size of the relation.
 */
typedef struct relation {
    tuple  *tuples;
    int32_t num_tuples;
} relation;
/**
 * Type definition for a relation.
 * It consists of an array of tuples and a size of the relation.
 */
typedef struct s_tuple {
    int32_t relation_R;
    int32_t relation_S;
} s_tuple;
/**
 * Type definition of a simple tuple.
 * It consists of the row IDs of the relations joined (in this case relation R and S).
 */
typedef struct result {
    s_tuple* results;
    struct result* next;
} result;
/**
 * Type definition of the results list.
 * It consists of the list of results and the link the next block.
 */


/**
 * Type definition of the results list, along with its size.
 */
typedef struct resultsWithNum {

    int results_amount;
    result* results;

} resultsWithNum;

/** Radix Hash Join**/
resultsWithNum* RadixHashJoin(relation *relR, relation *relS);

#endif
