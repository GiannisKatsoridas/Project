#include "Query.h"
#include "Tables.h"

#ifndef CARAMEL_JOINENUMERATION_H
#define CARAMEL_JOINENUMERATION_H

int connectedRelationsNum;
int joinComparisonsNum;

typedef struct PermutationsQueue {

    int* array;
    struct PermutationsQueue* next;

} PermutationsQueue;

typedef struct JEStatsByColumn {

    int u;
    int l;
    int f;
    int d;

} JEStatsByColumn;

typedef struct JEStatsByRelation {

    int columns_num;
    JEStatsByColumn* jesbc;

} JEStatsByRelation;

typedef struct JoinEnumerationStats {

    int relations_num;
    int* intermediateResults;       // An array of all the relations already joined.
    int intermediateResultsNum;
    int results_num;
    JEStatsByRelation* jesbr;

} JEStats;

/**
 * Create a bool 2D array (basically int**) of whether the two relations are connected or not.
 * @param query
 * @return
 */
int** createGraph(Query* query);

/**
 * Returns 1 if the relation is connected with the tree "relations" as seen from the graph.
 */
int isConnected(int** graph, int newRelation, int* relations, int relationsNum);

/**
 * Recursively fill the PermutationsQueue queue with int arrays of the various permutations of the
 * given array.
 */
void permute(int *array, int i, int length, PermutationsQueue* pq);

PermutationsQueue* PQInit();

int* PQPop(PermutationsQueue* pq);

void PQPush(PermutationsQueue* pq, int* array);

void PQDelete(PermutationsQueue* pq);

/**
 * Unique hash function to calculate position of permutation in the Cost hash table: A*4! + B*3! + C*2! + D*1!
 * (Given that the array given is [A, B, C, D])
 */
int hashFunction(int* array, int length, int relationsLength);

/**
 * Unique hash function to calculate position of permutation in the BestTree hash table! It calculated the position
 * inside the table by multiplying the positions where the relation is present with their respective index.
 * E.g. Relations in total: [0, 1, 2, 3]. Set to calculate: [1, 2, 3] (and [2, 3, 1] and all the others).
 * The boolean array would be [0, 1, 1, 1] since the first relation is not present but the other three are.
 * So the hashed value would be: 0*1! + 1*2! + 1*3! + 1*4!;
 */
int hashFunctionBestTree(int* array, int length);


/**
 * Simple factorial function.
 */
int factorial(int a);

/**
 * Simply create an incrementing array with the length "length". Example: Length = 4, Result = [0, 1, 2, 3]
 */
int* createArray(int length);


/**
 * https://www.geeksforgeeks.org/print-all-possible-combinations-of-r-elements-in-a-given-array-of-size-n/
 */
void combinationUtil(int arr[], int data[], int start, int end,
                     int index, int r, PermutationsQueue* pq);

void getCombination(int arr[], int n, int r, PermutationsQueue* pq);

/**
 * Initialize a JEStats struct with all the relations and columns of the query, as well as their initial values.
 */
JEStats* initializeJEStats(table** t, Query* q);

/**
 * Apply the necessary changes to the stats from the filters of the query.
 */
void applyInitialFilters(JEStats *jes, Query *q, table** t);

/**
 * Apply a specific greater than filter the a specific column of the greater JEStats struct.
 */
void executeGreaterThanFilter(JEStats *jes, Comparison comparison, table** t, int* relations);

/**
 * Apply a specific less than filter the a specific column of the greater JEStats struct.
 */
void executeLessThanFilter(JEStats *jes, Comparison comparison, table** t, int* relations);

/**
 * Apply a specific equals filter the a specific column of the greater JEStats struct.
 */
void executeEqualFilter(JEStats *jes, Comparison comparison, table** t, int* relations);

/**
 * If a comparison is being made between two columns of the same relation then it is considered to be a filter.
 */
void executeSameRelationFilter(JEStats *jes, Comparison comparison, table **t, int* relations);

/**
 * Returns 1 if a value exists in the given array of ints. Returns 0 otherwise.
 */
int existsInRelation(uint64_t *rel, int value, u_int64_t size);

/**
 * Same as the above, only with different integer sizes.
 */
int existsInRelationInt(int *rel, int value, int size);

/**
 * Calculates the factor of the distinct values from the given type: = dA * (1 − (1 − fA / f′A) ^ (f'A / d A))
 */
double calculateStatsDistinctsFactor(int f, int fA, int fC, int dC);

/**
 * Simple function to create a copy of the given JEStats to place to a new position in the Costs hash table.
 */
JEStats* copyJEStats(JEStats* jes);

/**
 * Join two columns of different relations into the new JEStats given as argument.
 */
void applyJoinToStats(JEStats *jes, int* arr, int arrLength, int newRel, Comparison_t *comparisons, table **t, int* relations);

/**
 * In case there are more than one joins of two relations within the same intermediate results, the second one
 * is perceived as a filter.
 */
void applyJoinToStatsSameRelation(JEStats *jes, Comparison comparison, table **t, int* relations);

/**
 * The case where the same column of the same relation is joined with itself.
 */
void applyJoinToStatsSameColumn(JEStats *jes, Comparison comparison, table **t, int* relations);

/**
 * Given the query, gets all the connecting nodes and creates an array of all the connected relations.
 */
int *createConnectedRelationsFromGraph(Query *q);

/**
 * Given the total comparisons and the best order of joins, returns an array of the best order of comparisons.
 */
int *getComparisonsOrder(Comparison_t *comparisons, int *bestTree, int* relations);

/**
 * The Join Enumeration Algorithm.
 */
int* JoinEnumeration(table** t, Query* q);


#endif //CARAMEL_JOINENUMERATION_H
