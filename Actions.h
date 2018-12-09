#ifndef _ACTIONS_H_
#define	_ACTIONS_H_

#include "Results.h"
#include "Tables.h"
#include "Query.h"
#include "DataParse.h"
static int intermediateResultsAmount = 0;

typedef struct IntermediateResults
{//struct for saving results between joins or comparisons
	uint64_t *tupleIDs;
	int *relationIDs;
    int32_t **keys;
    
    uint64_t tupleAmount;
    int relAmount;
} IntermediateResults;

typedef struct IntermediateResultsList{

	IntermediateResults* table;
	struct IntermediateResultsList* next;

} IntermediateResultsList ;

/**
 * Initializes an intermediate results struct.
 */
void IntermediateResultsInit(IntermediateResults **inRes);

/**
 * Given an intermediate results table, creates a relation struct to insert into the Radix algorithm, so it can
 * perform the join operation between two relations.
 */
relation *createRelationFromIntermediateResults(IntermediateResults* inRes, table *t, int relationID, int columnID);

/**
 * Allocates the necessary space for an intermediate results structure.
 */
void IntermediateResultsAlloc(IntermediateResults** inRes, uint64_t tupleAmount, int relAmount);

/**
 * Deletes a given intermediate results struct.
 */
void IntermediateResultsDel(IntermediateResults *inRes);

/**
 * Given a query q and a table array containing all the relations given as input, executes the query.
 */
void executeQuery(table **t, Query *q);

/**
 * In case the action required by the query is a comparison (>, <, =) then this functions is called, which
 * compares each line of the relation to find all the rows that satisfy comparison.
 */
IntermediateResultsList* compareColumn(IntermediateResultsList *list , table *t, int relationID, int columnID , int value , int action);

/**
 * Two columns of the same relation are joined and therefore there is no need to call the Radix algorithm. We must
 * simply compare the values of the two columns of the relation to find those that are equal.
 */
IntermediateResultsList* joinSameRelation(IntermediateResultsList* head, table **t, int relationA, int columnA, int columnB);

/**
 * Two different relations are joined and therefore the Radix algorithm must me called. If there are already
 * any intermediate results, they are taken into consideration. Otherwise, the initial relations' values are
 * inserted into the algorithm.
 */
IntermediateResultsList* joinRelationsRadix(IntermediateResultsList* inRes, table **t, int relationA, int relationB, int columnA, int columnB);

/**
 * There may be more than one different intermediate results tables, so this functions returns the index of the
 * table containing either of the 2 relations. If none are contained, the index -1 is returned. If the number
 * of intermediate results tables is 0, then a new table is created and the index 0 is returned. If two relations
 * are found on different intermediate results tables, then -2 is returned, meaning that two tables will need to
 * be merged.
 */
int getIntermediateResultsIndex(IntermediateResultsList* inRes, int relationA, int relationB);

/**
 * Returns 1 if the given relationID exists in the given IntermediateResults table. Returns 0 otherwise.
 */
int existsInIntermediateResults(IntermediateResults *inRes, int rel);

/**
 * If the relations being joined have already been joined on another column, then there is no need to run the Radix
 * algorithm. We simply need to traverse the intermediate results one by one and keep only those whose value in both
 * joined columns is the same.
 */
IntermediateResults* addResultsSameIntermediateResultsSize(table** t, IntermediateResults* inRes, int relationA, int columnA, int relationB, int columnB);

/**
 * If only one of the two relations being joined exists in one intermediate results table, then a new table must be
 * constructed, contaning one more column, the one for the new relation. Then each result from the radix algorithm is
 * placed accordingly into the new IntermediateResults table.
 */
IntermediateResults* addResultsWithNewColumn(result* results, IntermediateResults* inRes, int relationA, int relationB);

/**
 * If none of the two relations exists in any of the intermediate results tables, then a new must be created, that
 * contains nothing but the two relations currently being joined.
 */
IntermediateResults* addResultToNewIntermediateResult(result *results, IntermediateResults *inRes, int relationA, int relationB);

/**
 * If the 2 relations currently being joined are in different intermediate results tables, then those two tables need
 * to merge into one, so all the relations are connected to each other.
 */
IntermediateResults* mergeIntermediateResults(IntermediateResultsList* inRes, table** t, int relationA, int relationB, int columnA, int columnB);

/**
 * Given a join query and the current list of intermediate results, returns:
 * Category 0: No relation exists in any intermediate table
 * Category 1: One relation exists in one intermediate table and the other exists in none
 * Category 2: Both relations exist in the same intermediate table
 * Category 3: Both relations exist in different intermediate tables
 */
int getQueryCategory(IntermediateResultsList* inRes, int relationA, int relationB);

/**
 * Simple list creation - creates the intermediate results list.
 */
IntermediateResultsList* createList();

/**
 * Given a specific index, returns the node of the list, corresponding to that index.
 */
IntermediateResults* getNodeFromList(IntermediateResultsList* list, int index);

/**
 * Simple list insertion. Inserts a new node at the end of the list.
 */
IntermediateResultsList* addNodeToList(IntermediateResultsList* list, IntermediateResults* table);

/**
 * Simple list node deletion. Deletes the node at position index.
 */
void deleteNodeFromList(IntermediateResultsList* list, int index);

/**
 * Simple list deletion. Deletes the Intermediate results list.
 */
void deleteList(IntermediateResultsList** headaddr);

/**
 * Creates and returns a simple intermediate results structure.
 */
IntermediateResults* createIntermediateResult();

/**
 * Given an intermediate results lists, searches for the node that contains the relationA relation. If that node
 * is found then its index is returned. Otherwise, returns -1.
 */
int getIntermediateResultsSingleIndex(IntermediateResultsList* inRes, int relationA);

/**
 * Given the 2 relations of a join between relations of the same intermediate results table, returns the amount
 * of rows that are the same, therefore the result of the join.
 */
int calculateSameJoinResultsAmount(relation* relA, relation* relB);

/**
 * Prints all the contents of an intermediate results list after the actions have finished. Runs once per query.
 */
void printActionResults(table** t, IntermediateResultsList *inRes, Column_t *columns, int* rels);

/**
 * Given the columns need to be printed for a query, creates and returns an intermediate results table, containing
 * all the values of the intermediate results list and the table t. Decides whether to take the values from the
 * list of the table, on whether the relation exists in the list.
 */
IntermediateResults *getIntermediateResultFromColumns(table** t, IntermediateResultsList *inRes, Column_t *columns, int* rels);

/**
 * In case two not connected intermediate results tables need to be joined in order to print values from relations
 * from both of them, then a cross product of the two is created.
 */
IntermediateResults *crossProductIntermediateResults(IntermediateResults *inResA, IntermediateResults *inResB);

/**
 * If one relations does not exist in an intermediate results table, but need to be printed, then an intermediate
 * results table needs to be created, containing simply the column of the table with the rowIDs as keys.
 */
IntermediateResults* createIntermediateResultFromTable(table* t, int relation);

/**
 * Given an intermediate results table, returns the index of the relation 'relation' in the relationsID array and the
 * keys one.
 */
int getColumnIntermediateResultsIndex(IntermediateResults* inRes, int relation);

#endif