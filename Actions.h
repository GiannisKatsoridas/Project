#ifndef _ACTIONS_H_
#define	_ACTIONS_H_

#include "Results.h"
#include "Tables.h"
#include "Query.h"

static int intermediateResultsAmount = 0;

typedef struct IntermediateResults
{//struct for saving results between joins or comparisons
	uint64_t *tupleIDs;
	int *relationIDs;
    int32_t **keys;
    
    uint64_t tupleAmount;
    int relAmount;
} IntermediateResults;

/**
 * Executes queries.
 * @param t
 * @param q
 */

void IntermediateResultsInit(IntermediateResults **inRes);

relation *createRelationFromIntermediateResults(IntermediateResults* inRes, table *t, int relationID, int columnID);

void insertResultToIntermediateResults(IntermediateResults **inResAddr, result *results, int relationA, int relationB, int column);

void IntermediateResultsDel(IntermediateResults *inRes);


void executeQuery(table **t, Query *q);

void compareColumn(table *t , int colA , int value , int action);

void joinSameRelation(table *t, int columnA, int columnB);

/**
 * Two different relations are joined and therefore the Radix algorithm must me called. If there are already
 * any intermediate results, they are taken into consideration. Otherwise, the initial relations' values are
 * inserted into the algorithm.
 */
void joinRelationsRadix(IntermediateResults*** inRes, table **t, int relationA, int relationB, int columnA, int columnB);

/**
 * There may be more than one different intermediate results tables, so this functions returns the index of the
 * table containing either of the 2 relations. If none are contained, the index -1 is returned. If the number
 * of intermediate results tables is 0, then a new table is created and the index 0 is returned. If two relations
 * are found on different intermediate results tables, then -2 is returned, meaning that two tables will need to
 * be merged.
 */
int getIntermediateResultsIndex(IntermediateResults** inRes, int relationA, int relationB);

/**
 * Returns 1 if the given relationID exists in the given IntermediateResults table. Returns 0 otherwise.
 */
int existsInIntermediateResults(IntermediateResults *inRes, int rel);

/**
 * If the relations being joined have already been joined on another column, then there is no need to run the Radix
 * algorithm. We simply need to traverse the intermediate results one by one and keep only those whose value in both
 * joined columns is the same.
 */
IntermediateResults* addResultsSameIntermediateResultsSize(table** t, IntermediateResults* inRes, IntermediateResults* r, int relationA, int columnA, int relationB, int columnB);

/**
 * If only one of the two relations being joined exists in one intermediate results table, then a new table must be
 * constructed, contaning one more column, the one for the new relation. Then each result from the radix algorithm is
 * placed accordingly into the new IntermediateResults table.
 */
IntermediateResults* addResultsWithNewColumn(result* results, IntermediateResults* inRes, IntermediateResults* r, int relationA, int relationB);

/**
 * If none of the two relations exists in any of the intermediate results tables, then a new must be created, that
 * contains nothing but the two relations currently being joined.
 */
void addResultToNewIntermediateResult(result *results, IntermediateResults *inRes, int relationA, int relationB);

/**
 * Adds a new intermediate table at the end of the 2D table containing all the intermediate results.
 */
void addIntermediateResultsTable(IntermediateResults*** inRes);


void mergeIntermediateResults(IntermediateResults ***inRes, table** t, int relationA, int relationB, int columnA, int columnB);


int getQueryCategory(IntermediateResults** inRes, int relationA, int relationB);
#endif