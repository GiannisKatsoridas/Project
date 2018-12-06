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

int relationInIntermediateResults(IntermediateResults* inRes, int relationID)
{
	int pos = -1;
	for (int i = 0; i < inRes->relAmount; i++)
	{
		if(inRes->relationIDs[i] == relationID)
		{
			pos = i;
			break;
		}
	}
	return pos;
}

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

void insertResultToIntermediateResults(IntermediateResults **inResAddr, result *results, int relationA, int relationB, int column)
{
	if((*inResAddr) == NULL)
		IntermediateResultsInit(inResAddr);

	IntermediateResults *inRes = *inResAddr;

	int const results_num = getResultsAmount();
	int const tuples_per_page = getResultTuplesPerPage();

	if (inRes->relAmount == 0)//first result that is inserted to the struct!
	{

		inRes -> tupleIDs = malloc(results_num * sizeof(uint64_t));
		inRes -> relationIDs = malloc(2 * sizeof(int));
		inRes -> keys = malloc(2 * sizeof(int32_t*));
		for (int i = 0; i < 2; i++)
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

		//check for each relation if it exists in the previous results
		int const relationApos = relationInIntermediateResults(inRes, relationA);
    	int const relationBpos = relationInIntermediateResults(inRes, relationA);

    	if((relationApos == -1)&&(relationBpos == -1))
    	{
    		fprintf(stderr, "Relations %d and %d don't exist in IntermediateResults\n", relationA, relationB);
    		return;
    	}
		//create a histogram of all the tuple keys 'i'
		int tupleIDhist[inRes->tupleAmount];
		for (int i = 0; i < inRes->tupleAmount; i++)
		{
			tupleIDhist[i] = 0;
		}


		result *r1 = results;
		int i = 0;
		int counter = 0;
		while(results != NULL)
		{
			for (i = 0; (i<tuples_per_page) && (counter < results_num); i++)
			{
				if(column == 0)
					tupleIDhist[results->results[i].relation_R] ++;
				else if(column == 1)
					tupleIDhist[results->results[i].relation_S] ++;
				counter++;
			}
			results = results->next;
		}

		results = r1;

		//create accumulative histogram for the old tuple keys 'i'
		int tupleIDpsum[inRes->tupleAmount];
		int sum = 0;
		for(i=0; i<inRes->tupleAmount; i++)
		{
	        tupleIDpsum[i] = sum;
	        sum += tupleIDhist[i];
    	}

    	
    	//push results to intermediate results
    	IntermediateResults *inResNew ;//= malloc(sizeof(IntermediateResults));
    	IntermediateResultsInit(&inResNew);

    	inResNew -> tupleIDs = malloc(sum*sizeof(uint64_t));

    	if( (relationApos != -1) && (relationBpos !=-1))//if both recently joined-relations exist in previous results
    		inResNew -> relAmount = inRes -> relAmount +1;
		else//one of them exists in intermediate results 
			inResNew -> relAmount = inRes -> relAmount;
		//(if none of them exists, there is a return condition above)


		inResNew -> relationIDs = malloc((inResNew -> relAmount)*sizeof(int));
		inResNew -> keys = malloc(inResNew -> relAmount * sizeof(int32_t*));
		for (int i = 0; i < inResNew -> relAmount; i++)
		{
			inResNew -> keys[i] = malloc(results_num * sizeof(int32_t));
		}


		//add valid old relation IDs to the new intermediate results
		for (int i = 0; i < inRes -> relAmount; i++)
		{
			inResNew -> tupleIDs[i] = inRes -> tupleIDs[i];
		}


		//add valid old results to the new intermediate results
		int newtupleID = 0;
		for (int tup = 0; tup < inRes -> tupleAmount ; tup++)//for each old tuple
		{
			for(int i = 0 ; i< tupleIDhist[tup] ; i++)//for how many times it is repeated in the new results
			{
				//new tupleID
				newtupleID = tupleIDpsum[tup] + i;
				inResNew -> tupleIDs[newtupleID] = newtupleID;
				for (int relIndx = 0; relIndx < inRes -> relAmount; relIndx++)
				{
					//need to add sth if new relation is added
					inResNew -> keys[relIndx][newtupleID] = inRes -> keys[relIndx][tup];
				}
			}
		}

		if ((relationApos == -1) && (relationBpos !=-1))
		{//relation A included in result!
			//add its ID to Intermediate result, at the end of the list
			inResNew -> tupleIDs[inResNew->tupleAmount -1] = relationA;	
		}
		else if ((relationApos != -1) && (relationBpos ==-1))
		{//relation B included in result!
			inResNew -> tupleIDs[inResNew->tupleAmount -1] = relationB;	
		}

		if (((relationApos == -1) && (relationBpos !=-1)) || ((relationApos == -1) && (relationBpos !=-1)))
		{
			int i = 0;
			int counter = 0;
			while(results != NULL)
			{
				for (i = 0; (i<tuples_per_page) && (counter < results_num); i++)
				{
					if(column == 0)
					{
						inResNew -> keys[inResNew->tupleAmount -1][tupleIDpsum[results->results[i].relation_R]] = results->results[i].relation_S;
						tupleIDpsum[results->results[i].relation_R] ++;
					}
					else if(column == 1)
					{
						inResNew -> keys[inResNew->tupleAmount -1][tupleIDpsum[results->results[i].relation_S]] = results->results[i].relation_R;
						tupleIDpsum[results->results[i].relation_S] ++;
					}
					counter++;
				}
				results = results->next;
			}
		}

		IntermediateResultsDel(inResAddr);
		*inResAddr = inResNew;
}
}

void IntermediateResultsDel(IntermediateResults **inRes)
{
	free((*inRes) ->tupleIDs);
	free((*inRes) ->relationIDs);
	free((*inRes) ->keys);
	free((*inRes));
	(*inRes) == NULL;
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

