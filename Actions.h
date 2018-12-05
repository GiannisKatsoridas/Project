#ifndef _ACTIONS_H_
#define	_ACTIONS_H_

#include "Results.h"

/**
 * Executes queries.
 * @param t
 * @param q
 */
void executeQuery(table **t, Query *q);

void compareColumn(table *t , int colA , int value , int action);

void joinSameRelation(table *t, int columnA, int columnB);

void joinRelationsRadix(table *tA, table *tB, int columnA, int columnB);


#endif