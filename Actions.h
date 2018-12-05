#ifndef _ACTIONS_H_
#define	_ACTIONS_H_

#include "Globals.h"

#include "Query.h"
#include "Tables.h"
#include "Results.h"

void executeQueries(table **t, Query *q);

void compareColumn(table *t , int colA , int value , int action);


#endif