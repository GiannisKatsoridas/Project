#include <stdint.h>

//#define CACHE_SIZE 3145728;                             // Processor Cache
#define CACHE_SIZE 10000;
#define RELR_SIZE 1000;                                 // Relation R size
#define RELS_SIZE 1000;                                 // Relation S size
#define MAX_VALUE 100;                                  // Max value that the relation values can go to

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
    uint32_t num_tuples;
} relation;
/**
 * Type definition for a relation.
 * It consists of an array of tuples and a size of the relation.
 */
typedef struct result {
//    ...
} result;
/** Radix Hash Join**/
result* RadixHashJoin(relation *relR, relation *relS);

relation* relation_R;
relation* relation_S;