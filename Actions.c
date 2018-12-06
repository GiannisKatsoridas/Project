#include <stdio.h>
#include <stdlib.h>

#include "Actions.h"

void executeQuery(table **t, Query *q)
{
	//parseTableData(t);

	int actions = q->comparison_set->comparisons_num;
	int* rels = q->query_relation_set->query_relations;
	Comparison *cmp = NULL;
	for (int i = 0; i < actions; i++)
	{
		cmp = &(q->comparison_set->comparisons[i]);
		if(cmp->action != JOIN)
		{
			compareColumn(t[rels[cmp->relationA]] , cmp->columnA , cmp->relationB , cmp->action);
		}
		else if(cmp->action == JOIN)
		{
			if (cmp->relationA == cmp->relationB)
			{
				joinSameRelation(t[rels[cmp->relationA]], cmp->columnA, cmp->columnB);
			}
			else
			{
				joinRelationsRadix(t[rels[cmp->relationA]], t[rels[cmp->relationB]], cmp->columnA, cmp->columnB);
			}
		}
		else
		{
			fprintf(stderr, "wtf\n");
		}
	}
}


void IntermediateResultsInit(IntermediateResults **inRes)
{
	(*inRes) = malloc(sizeof(IntermediateResults));
	(*inRes) -> tupleIDs = NULL;
	(*inRes) -> relationIDs = NULL;
	(*inRes) -> keys = NULL;

	(*inRes) -> tupleAmount = 0;
	(*inRes) -> relAmount = 0;
}


relation* createRelationFromTable(table *t, int columnID)
{
    if ((t==NULL) || (columnID < 0))
    {
        printf("createRelationFromTable(): NULL table\n");
        return NULL;
    }

    if ((columnID > (t->size-1) ) || (columnID < 0))
    {
        printf("createRelationFromTable(): invalid columnID %d\n", columnID);
        return NULL;
    }

    //create relation
    relation* rel = malloc(sizeof(relation));
    rel->num_tuples = t -> size;

    //assign to the relation tuples the payloads from the columnID of the table
    rel->tuples = malloc(rel->num_tuples * sizeof(tuple));
    for(int i=0; i<rel->num_tuples; i++)
    {
        rel->tuples[i].key = i;
        rel->tuples[i].payload = t->columns[columnID][i];
    }
}

/*int relationExistsInIntermediateResults(IntermediateResults* inRes, int relationID)
{
	int flag = 0;
	for (int i = 0; i < inRes->relAmount; i++)
	{
		if(inRes->relationIDs[i] == relationID)
		{
			flag = 1;
			break;
		}
	}
	return flag;
}*/

relation *createRelationFromIntermediateResults(IntermediateResults* inRes, table *t, int relationID, int columnID)
{
	if ((t==NULL) || (columnID < 0))
    {
        printf("createRelationFromIntermediateResults(): NULL table\n");
        return NULL;
    }

    if ((columnID > (t->size-1) ) || (columnID < 0))
    {
        printf("createRelationFromIntermediateResults(): invalid columnID %d\n", columnID);
        return NULL;
    }

	if (inRes == NULL)
	{
		return (createRelationFromTable(t,columnID));
	}

	if ((inRes -> relAmount == 0) || (inRes -> tupleAmount == 0))
	{
		return (createRelationFromTable(t,columnID));
	}
	
	int relKeyArray = -1;
	//find key array for the correct relation
	for (int i = 0; i < inRes->relAmount; i++)
	{
		if(relationID == inRes->relationIDs[i])
		{
			relKeyArray = i;
			break;
		}
	}

	if (relKeyArray == -1)
	{
		fprintf(stderr, "createRelationFromIntermediateResults(): relationID doesn't exist in results\n");
		return NULL;//PROBLEM if relations are not joined in a chain
	}

	//create relation, using existing result's tupleIDs instead of relation keys
	relation *rel = malloc(sizeof(relation));
	rel -> num_tuples = (int32_t) inRes -> tupleAmount;

	rel -> tuples = malloc((rel->num_tuples) * sizeof(tuple));
	for (int i = 0; i < rel->num_tuples; i++)
	{
		rel->tuples[i].key = (int32_t) inRes->tupleIDs[i];
		rel->tuples[i].payload = (int32_t) t->columns[columnID][inRes->keys[relKeyArray][i]];
	}

	return rel;
}

void insertResultToIntermediateResults(IntermediateResults *inRes, result *results, int relationA, int relationB, int column)
{


	int const results_num = getResultsAmount();
	int const tuples_per_page = getResultTuplesPerPage();

	if (inRes->relAmount == 0)//first result that is inserted to the struct!
	{

		inRes -> tupleIDs = malloc(results_num * sizeof(uint64_t));
		inRes -> relationIDs = malloc(2 * sizeof(int));
		inRes -> keys = malloc(2 * sizeof(int32_t*));
		for (int i = 0; i < 2; ++i)
		{
			inRes -> keys[i] = malloc(results_num * sizeof(int32_t));
		}

		inRes -> tupleAmount = (uint64_t) results_num;
		inRes -> relAmount = 2;


		inRes -> relationIDs[0] = relationA;
		inRes -> relationIDs[1] = relationB;

		for (uint64_t i = 0; i < inRes -> tupleAmount; i++)
		{
			inRes -> tupleIDs[i] = i;
			inRes -> keys[0][i] = results->results[i].relation_R;
			inRes -> keys[1][i] = results->results[i].relation_S;
		}

		uint64_t i = 0;
		int counter = 0;
		while(results != NULL)
		{
			for (i = 0; (i<tuples_per_page) && (counter < results_num); i++)
			{
				inRes -> tupleIDs[counter] = counter;
				inRes -> keys[0][counter] = results->results[i].relation_R;
				inRes -> keys[1][counter] = results->results[i].relation_S;
				counter++;
			}
			results = results->next;
		}
	}
	else//there are already results in this struct, we need to merge them with the new ones
	{
		//IF IT EXISTS, THEN ??

		//create a histogram of all the tuple keys 'i'
		int tupleIDcounters[inRes->tupleAmount];
		for (int i = 0; i < inRes->tupleAmount; i++)
		{
			tupleIDcounters[i] = 0;
		}


		result *r1 = results;
		int i = 0;
		int counter = 0;
		while(results != NULL)
		{
			for (i = 0; (i<tuples_per_page) && (counter < results_num); i++)
			{
				if(column == 0)
					tupleIDcounters[results->results[i].relation_R] ++;
				else if(column == 1)
					tupleIDcounters[results->results[i].relation_S] ++;
				counter++;
			}
			results = results->next;
		}

		results = r1;

		//create accumulative histogram for the old tuple keys 'i'
		int psumIDcounters[inRes->tupleAmount];
		int sum = 0;
		for(i=0; i<inRes->tupleAmount; i++)
		{
	        psumIDcounters[i] = sum;
	        sum += tupleIDcounters[i];
    	}


    	IntermediateResults *inResNew = malloc(sizeof(IntermediateResults));
    	inResNew -> tupleIDs = malloc(sum*sizeof(uint64_t));
    	inResNew -> relationIDs = malloc((inRes -> relAmount +1)*sizeof(int));
	}
}

void IntermediateResultsDel(IntermediateResults **inRes)
{
	free((*inRes) ->tupleIDs);
	free((*inRes) ->relationIDs);
	free((*inRes) ->keys);
}

void compareColumn(table *t , int colA , int value , int action)
{
	//fprintf(stdout, "%d %d %d\n", colA, value, action);
	/*int counter = 0;
	int32_t keytemp;
	for (int i = 0; i < t->inRes->amount; i++)
	{
		keytemp = t->inRes->keys[i];
		switch(action)
		{
			case(EQUAL):
				if (t->columns[colA][keytemp] == value)
					counter ++;
				break;
			case(LESS_THAN):
				if (t->columns[colA][keytemp] < value)
					counter ++;
				break;
			case(GREATER_THAN):
				if (t->columns[colA][keytemp] > value)
					counter ++;
				break;
			default:
				fprintf(stderr, "wut\n");
		}
	}

	int32_t *keys = malloc(counter * sizeof(int32_t));
	int k = 0;
	for (int i = 0; (i < t->inRes->amount) && (k < counter); i++)
	{
		keytemp = t->inRes->keys[i];
		switch(action)
		{
			case(EQUAL):
				if (t->columns[colA][keytemp] == value)
				{
					keys[k] = i;
					k++;
				}
				break;
			case(LESS_THAN):
				if (t->columns[colA][keytemp] < value)
				{
					keys[k] = i;
					k++;
				}
				break;
			case(GREATER_THAN):
				if (t->columns[colA][keytemp] > value)
				{
					keys[k] = i;
					k++;
				}
				break;
			default:
				fprintf(stderr, "wut\n");
		}
	}

	free(t->inRes->keys);

	t->inRes->keys = keys;
	t->inRes->amount = counter;*/
}

void joinSameRelation(table *t, int columnA, int columnB) {

    /*int counter = 0;
    int32_t keytemp;
    for (int i = 0; i < t->inRes->amount; i++)
    {
        keytemp = t->inRes->keys[i];
        if(t->columns[columnA][keytemp] == t->columns[columnB][keytemp])
            counter++;
    }

    int32_t* keys = malloc(counter * sizeof(int32_t));

    int k = 0;
    for (int i = 0; (i < t->inRes->amount) && (k < counter); i++)
    {
        keytemp = t->inRes->keys[i];
        if(t->columns[columnA][keytemp] == t->columns[columnB][keytemp]){
            keys[k] = keytemp;
            k++;
        }
    }

    free(t->inRes->keys);

    t->inRes->keys = keys;
    t->inRes->amount = counter;*/
}



void joinRelationsRadix(table *tA, table *tB, int columnA, int columnB) {

    /*relation* relA = constructRelationForNextJoin(tA, columnA);
    relation* relB = constructRelationForNextJoin(tB, columnB);

    result* results = RadixHashJoin(relA, relB);

    saveResult(tA, tB, results);*/
}

