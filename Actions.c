#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "Actions.h"


void executeQuery(table **t, Query *q)
{
	int actions = q->comparison_set->comparisons_num;
	int* rels = q->query_relation_set->query_relations;
	Comparison *cmp = NULL;
	IntermediateResultsList* inRes;
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
				inRes = joinRelationsRadix(inRes, t, rels[cmp->relationA], rels[cmp->relationB], cmp->columnA, cmp->columnB);
				if(i==1) {
					printf("RUN\n");
					//printf("%d\n", inRes[1]->relAmount);
					printf("RUN\n");
				}
			}
		}
		else
		{
			fprintf(stderr, "wtf\n");
		}

		setResultsAmount(0);
	}

	inRes = inRes->next;

	printf("%d - %d\n", inRes->table->relationIDs[0], inRes->table->relationIDs[1]);
	for(int j=0; j<inRes->table->tupleAmount; j++){
		printf("%d\t%d\n", inRes->table->keys[0][j], inRes->table->keys[1][j]);
	}
	inRes = inRes->next;
	printf("NEW TABLE: %d\n", inRes->table->relAmount);
	printf("%d - %d\n", inRes->table->relationIDs[0], inRes->table->relationIDs[1]);
	for(int j=0; j<inRes->table->tupleAmount; j++){
		printf("%d\t%d\n", inRes->table->keys[0][j], inRes->table->keys[1][j]);
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
    rel->num_tuples = (int32_t) t -> size;

    //assign to the relation tuples the payloads from the columnID of the table
    rel->tuples = malloc(rel->num_tuples * sizeof(tuple));
    for(int i=0; i<rel->num_tuples; i++)
    {
        rel->tuples[i].key = i;
        rel->tuples[i].payload = (int32_t) t->columns[columnID][i];
    }

    return rel;
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
	int i, index=0;

	for(i=0; i<inRes->relAmount; i++) {

		if (inRes->relationIDs[i] == relationID) {
			index = i;
			break;
		}

	}

	if(i == inRes->relAmount)
	    return NULL;

	relation *rel = malloc(sizeof(relation));
	rel -> num_tuples = (int32_t) inRes -> tupleAmount;

	rel -> tuples = malloc((rel->num_tuples) * sizeof(tuple));
	for (i = 0; i < rel->num_tuples; i++)
	{
		rel->tuples[i].key = i;
		rel->tuples[i].payload = (int32_t) t->columns[columnID][inRes->keys[index][i]];
	}

	return rel;
}

void IntermediateResultsDel(IntermediateResults *inRes)
{
		free(inRes->tupleIDs);
		free(inRes->relationIDs);
		free(inRes->keys);
		free(inRes);
		inRes = NULL;
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


IntermediateResultsList* joinRelationsRadix(IntermediateResultsList* head, table **t, int relationA, int relationB, int columnA, int columnB){

	IntermediateResultsList* node;

	if(intermediateResultsAmount == 0)
		head = createList();

	int cat = getQueryCategory(head, relationA, relationB);
	int index = 0;

	if(cat == 0){											//	No intermediate results table has any of the two
															// 	relations
		//node = addNodeToList(head, createIntermediateResult());

		index = intermediateResultsAmount;

	}
	else if(cat == 3){

		//mergeIntermediateResults(inRes, t, relationA, relationB, columnA, columnB);
		return head;

	}
	else {
		index = getIntermediateResultsIndex(head, relationA, relationB);
	}

	IntermediateResults* r;										//	Create and initialize the new IntermediateResults
	IntermediateResultsInit(&r);								// 	table.

	/**
	 * If both relations exist in a single IntermediateResults table then act accordingly.
	 */
	if(existsInIntermediateResults(getNodeFromList(head, index), relationA) && existsInIntermediateResults(getNodeFromList(head, index), relationB)) {
		IntermediateResults *temp = addResultsSameIntermediateResultsSize(t, getNodeFromList(head, index), relationA, columnA, relationB, columnB);
		deleteNodeFromList(head, index);
		addNodeToList(head, temp);
		return head;
	}

	relation *relA, *relB;

	/**
	 * If no relations exist in any intermediate results tables, then take all values from the table t.
	 */
	if(cat == 0){
		relA = createRelationFromTable(t[relationA], columnA);
		relB = createRelationFromTable(t[relationB], columnB);
	}
	else {
		/**
		 * Form the relations given as arguments to the Radix algorithm through the inRes table.
		 */
		relA = createRelationFromIntermediateResults(getNodeFromList(head, index), t[relationA], relationA, columnA);
		relB = createRelationFromIntermediateResults(getNodeFromList(head, index), t[relationB], relationB, columnB);

		if(relA == NULL)
            relA = createRelationFromTable(t[relationA], columnA);
		else if(relB == NULL)
		    relB = createRelationFromTable(t[relationB], columnB);
	}

	result* results = RadixHashJoin(relA, relB);

	/**
	 * If any relations exist in any intermediate results tables then add a new column to the intermediate results table
	 */
	if(existsInIntermediateResults(getNodeFromList(head, index), relationA) || existsInIntermediateResults(getNodeFromList(head, index), relationB)) {
		IntermediateResults* temp = addResultsWithNewColumn(results, getNodeFromList(head, index), relationA, relationB);
		deleteNodeFromList(head, index);
		addNodeToList(head, temp);
		return head;
	}

	/**
	 * Fill the newly created empty IntermediateResults table.
	 */
	IntermediateResults* temp = addResultToNewIntermediateResult(results, createIntermediateResult(), relationA, relationB);

	addNodeToList(head, temp);

	return head;

	//if(index == 1)
		//printf("-----%d-----\n", (*inRes[1])->relAmount);
}


void mergeIntermediateResults(IntermediateResults ***inRes, table** t, int relationA, int relationB, int columnA, int columnB) {

    int indexA = 0, indexB = 0;

    for (int i = 0; i < intermediateResultsAmount; i++) {
        if (existsInIntermediateResults(*inRes[i], relationA))
            indexA = i;
        if (existsInIntermediateResults(*inRes[i], relationB))
            indexB = i;
    }

    relation* relA = createRelationFromIntermediateResults(*inRes[indexA], t[relationA], relationA, columnA);
    relation* relB = createRelationFromIntermediateResults(*inRes[indexB], t[relationB], relationB, columnB);

    result* results = RadixHashJoin(relA, relB);

    IntermediateResults* r;
    IntermediateResultsInit(&r);

    r->relAmount = (*inRes[indexA])->relAmount + (*inRes[indexB])->relAmount;
    for(int i=0; i<(*inRes[indexA])->relAmount; i++)
    	r->relationIDs[i] = (*inRes[indexA])->relationIDs[i];
    for(int i=(*inRes[indexA])->relAmount; i<r->relAmount; i++)
    	r->relationIDs[i] = (*inRes[indexB])->relationIDs[i - (*inRes[indexA])->relAmount];
    r->tupleAmount = (uint64_t) getResultsAmount();

    for(int j=0; j<r->tupleAmount; j++){

		for(int i=0; i<(*inRes[indexA])->relAmount; i++)
			r->keys[i][j] = (*inRes[indexA])->keys[i][results->results[j%getResultTuplesPerPage()].relation_R];
		for(int i=(*inRes[indexA])->relAmount; i<r->relAmount; i++)
			r->keys[i][j] = (*inRes[indexB])->keys[i - (*inRes[indexA])->relAmount][results->results[j%getResultTuplesPerPage()].relation_S];

    }

    intermediateResultsAmount--;
    int flag = 0;

    IntermediateResults** newInRes;
    newInRes = malloc(intermediateResultsAmount*sizeof(IntermediateResults*));
	int k = 0;

    for(int i=0; i<intermediateResultsAmount; i++){

    	if(i == indexA || i == indexB){
    		if(flag == 0) {
				newInRes[i] = r;
				flag = 1;
    		}
    	}
    	else {
			newInRes[i] = *inRes[k];
		}
    	k++;

    }

	for(int i=0; i<intermediateResultsAmount; i++)
		IntermediateResultsDel(*inRes[i]);

	free(inRes);

	*inRes = newInRes;
}


IntermediateResults* addResultToNewIntermediateResult(result *results, IntermediateResults *inRes, int relationA, int relationB) {

	inRes->relAmount = 2;
	inRes->relationIDs = malloc(2*sizeof(int32_t));
	inRes->relationIDs[0] = relationA;
	inRes->relationIDs[1] = relationB;
	inRes->tupleAmount = (uint64_t) getResultsAmount();

	inRes->keys = malloc(2*sizeof(int32_t*));
	inRes->keys[0] = malloc(inRes->tupleAmount * sizeof(int32_t));
	inRes->keys[1] = malloc(inRes->tupleAmount * sizeof(int32_t));

	for(int i=0; i<inRes->tupleAmount; i++){

		inRes->keys[0][i] = results->results[i%getResultTuplesPerPage()].relation_R;
		inRes->keys[1][i] = results->results[i%getResultTuplesPerPage()].relation_S;

		if(i%getResultTuplesPerPage() == 0 && i != 0)
			results = results->next;
	}

	return inRes;

}


int existsInIntermediateResults(IntermediateResults *inRes, int rel) {

	if(inRes == NULL)
		return 0;

	for(int i=0; i<inRes->relAmount; i++){

		if(inRes->relationIDs[i] == rel)
			return 1;

	}

	return 0;
}


int getIntermediateResultsIndex(IntermediateResultsList* inRes, int relationA, int relationB) {

	for(int i=0; i<intermediateResultsAmount; i++)
		if(existsInIntermediateResults(getNodeFromList(inRes, i), relationA)){
			for(int j=0; j<intermediateResultsAmount; j++){
				if(existsInIntermediateResults(getNodeFromList(inRes, i), relationB) && i != j)
					return -2;
			}
			return i;
		}
		else if(existsInIntermediateResults(getNodeFromList(inRes, i), relationB)){
			for(int j=0; j<intermediateResultsAmount; j++){
				if(existsInIntermediateResults(getNodeFromList(inRes, i), relationA) && i != j)
					return -2;
			}
			return i;
		}

	return -1;

}


IntermediateResults* addResultsSameIntermediateResultsSize(table** t, IntermediateResults* inRes, int relationA, int columnA, int relationB, int columnB){

	IntermediateResults* r = createIntermediateResult();

	r->relAmount = inRes->relAmount;
	r->relationIDs = malloc(sizeof(int)*inRes->relAmount);
	for(int i=0; i<inRes->relAmount; i++)
		r->relationIDs[i] = inRes->relationIDs[i];

	int count = 0;
	int relAIndex = 0, relBIndex = 0;

	for(int i=0; i<inRes->relAmount; i++){

		if(inRes->relationIDs[i] == relationA)
			relAIndex = i;
		else if(inRes->relationIDs[i] == relationB)
			relBIndex = i;

	}

	for(int i=0; i<inRes->tupleAmount; i++){

		if(t[relationA]->columns[columnA][inRes->keys[relAIndex][i]] == t[relationB]->columns[columnB][inRes->keys[relBIndex][i]])
			count++;


	}

	r->tupleAmount = (uint64_t) count;
	r->keys = malloc(r->relAmount*sizeof(int32_t*));

	for(int i=0; i<r->relAmount; i++){
		r->keys[i] = malloc(count*sizeof(int32_t));
	}

	int newInResIndex = 0;
	for(int i=0; i<inRes->tupleAmount; i++){

		if(t[relationA]->columns[columnA][inRes->keys[relAIndex][i]] == t[relationB]->columns[columnB][inRes->keys[relBIndex][i]]){

			for(int j=0; j<r->relAmount; j++)
				r->keys[j][newInResIndex] = inRes->keys[j][i];

			newInResIndex++;

			if(newInResIndex == count)
				break;
		}

	}

	IntermediateResultsDel(inRes);

	return r;

}


IntermediateResults* addResultsWithNewColumn(result* results, IntermediateResults* inRes, int relationA, int relationB){

	int relIndex;

	IntermediateResults* r = createIntermediateResult();

	if(existsInIntermediateResults(inRes, relationA))
		relIndex = relationA;
	else
		relIndex = relationB;

	r->relAmount = inRes->relAmount + 1;
	r->relationIDs = malloc(sizeof(int)*r->relAmount);
	for(int i=0; i<inRes->relAmount; i++)
		r->relationIDs[i] = inRes->relationIDs[i];
	r->relationIDs[r->relAmount-1] = relIndex == relationA ? relationB : relationA;

	r->tupleAmount = (uint64_t) getResultsAmount();
	r->keys = malloc(r->relAmount*sizeof(int32_t*));

	for(int i=0; i<r->relAmount; i++)
		r->keys[i] = malloc(r->tupleAmount*sizeof(int32_t));

	for(int i=0; i<r->tupleAmount; i++){

		for(int j=0; j<inRes->relAmount; j++){

			r->keys[j][i] = relIndex == relationA ? inRes->keys[j][results->results[i % getResultTuplesPerPage()].relation_R] : inRes->keys[j][results->results[i % getResultTuplesPerPage()].relation_S];

		}
		r->keys[r->relAmount-1][i] = relIndex == relationA ? results->results[i % getResultTuplesPerPage()].relation_S : results->results[i % getResultTuplesPerPage()].relation_R;

		if(i%getResultTuplesPerPage() == 0 && i != 0)
			results = results->next;
	}

	IntermediateResultsDel(inRes);

	return r;
}


void addIntermediateResultsTable(IntermediateResults*** inRes){

	IntermediateResults** temp = malloc((intermediateResultsAmount)* sizeof(IntermediateResults*));

	for(int i=0; i<intermediateResultsAmount; i++)
		temp[i] = *inRes[i];

	*inRes = malloc((intermediateResultsAmount+1)* sizeof(IntermediateResults*));
	for(int i=0; i<intermediateResultsAmount; i++)
		*inRes[i] = temp[i];

	IntermediateResultsInit(inRes[intermediateResultsAmount]);

	printf("%d\n", intermediateResultsAmount);

	intermediateResultsAmount++;
}

/**
 * Category 0: No relation exists in any intermediate table
 * Category 1: One relation exists in one intermediate table and the other exists in none
 * Category 2: Both relations exist in the same intermediate table
 * Category 3: Both relations exist in different intermediate tables
 */
int getQueryCategory(IntermediateResultsList* inRes, int relationA, int relationB){

	int flag = 0;

	for(int i=0; i<intermediateResultsAmount; i++){

		if(existsInIntermediateResults(getNodeFromList(inRes, i), relationA) && existsInIntermediateResults(getNodeFromList(inRes, i), relationB))
			return 2;

		if(existsInIntermediateResults(getNodeFromList(inRes, i), relationA) || existsInIntermediateResults(getNodeFromList(inRes, i), relationB))
			if(!flag) {
				flag = 1;
			}
			else {
				return 3;
			}
	}

	if(flag)
		return 1;
	else
		return 0;

}


IntermediateResultsList* createList(){

	IntermediateResultsList* list = malloc(sizeof(IntermediateResultsList));
	list->next = NULL;
	list->table = malloc(sizeof(IntermediateResults));

	intermediateResultsAmount = 0;

	return list;
}


IntermediateResults* getNodeFromList(IntermediateResultsList* list, int index){

	IntermediateResultsList* curr = list->next;

	while(index > 0){

		curr = curr->next;
		index--;

	}

	return curr == NULL ? NULL : curr->table;
}


IntermediateResultsList* addNodeToList(IntermediateResultsList* list, IntermediateResults* table){

	IntermediateResultsList* curr = list;

	while(curr->next != NULL)
		curr = curr->next;

	curr->next = malloc(sizeof(IntermediateResultsList));
	curr->next->next = NULL;
	curr->next->table = table;

	intermediateResultsAmount++;

	return curr->next;
}


void deleteNodeFromList(IntermediateResultsList* list, int index) {

	if (list->next->next == NULL){
		list->next = NULL;
		return;
	}

	IntermediateResultsList* curr = list->next;

	while(index > 1) {
		curr = curr->next;
		index--;
	}

	if(curr->next == NULL)
		curr = NULL;
	else
		curr->next = curr->next->next;
}


IntermediateResults* createIntermediateResult(){

	IntermediateResults* table;
	IntermediateResultsInit(&table);

	return table;
}

