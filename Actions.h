#ifndef _ACTIONS_H_
#define	_ACTIONS_H_

#include "Results.h"
#include "Tables.h"
#include "Query.h"


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

void IntermediateResultsDel(IntermediateResults **inRes);


void executeQuery(table **t, Query *q);

void compareColumn(table *t , int colA , int value , int action);

void joinSameRelation(table *t, int columnA, int columnB);

void joinRelationsRadix(table *tA, table *tB, int columnA, int columnB);


#endif