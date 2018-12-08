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
			inRes = compareColumn(inRes, t[rels[cmp->relationA]] , cmp->relationA, cmp->columnA , cmp->relationB , cmp->action);
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

	//inRes = inRes->next;
	IntermediateResultsList* l = inRes;
	while(l != NULL)
	{
		for (int j = 0; j < l->table->relAmount; j++)
		{
			printf("(%5d)\t", l->table->relationIDs[j]);
		}
		printf("\n");

		for (int i = 0; i < l->table->tupleAmount; i++)
		{
			for (int j = 0; j < l->table->relAmount; j++)
			{
				printf("(%5d)\t", l->table->keys[j][i]);
			}
			printf("\n");
		}
		printf("---------------------------------------\n");
		l = l->next;		
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

int comparePayloadToValue(int32_t payload, int value, int action)
{
	switch(action){
		case(EQUAL):
			return (payload == value);
		case(LESS_THAN):
			return(payload < value);
		case(GREATER_THAN):
			return(payload > value);
		default:
			fprintf(stderr, "comparePayloadToValue(): invalid action\n");
			return (-1);
	}
}

int calculateActionResultAmount(relation *rel, int value, int action)
{
	int counter = 0;
	for (int i = 0; i < rel->num_tuples; i++)
	{
		if(comparePayloadToValue(rel->tuples[i].payload, value, action))
			counter++;
	}
	return counter;
}

IntermediateResultsList* compareColumn(IntermediateResultsList *list , table *t, int relationID, int columnID , int value , int action)
{
	if ((list == NULL) || (t == NULL) || (relationID<0) || (columnID < 0) )
	{
		fprintf(stderr, "compareColumn(): invalid argument(s)\n");
		return list;
	}

	if ((action != EQUAL) && (action != LESS_THAN) && (action != GREATER_THAN))
	{
		fprintf(stderr, "compareColumn(): invalid action\n");
		return list;
	}
	printf("%d.%d (%d) %d\n", relationID, columnID, action, value);

	IntermediateResultsList* templist = list;
	int cnt = intermediateResultsAmount;
	while((templist != NULL)&&(cnt))
	{
		if(existsInIntermediateResults(templist->table, relationID))
			break;
		templist = templist -> next;
		cnt--;
	}

	if(templist != NULL)//relation exists in an intermediate results table
	{//we need to remove the tuples that do not match the comparison result
		printf("Relation exists!\n");
		//first, create a relation from the existing results
		//each key of the relation will be the tupleID of the intermediate result tuple
		relation *rel = createRelationFromIntermediateResults(templist->table, t, relationID, columnID);

		//then, count how many of the rows meet the action requirement
		int newSize = calculateActionResultAmount(rel, value, action);

		//then, create a new Intermediate Results table with newSize tuples
		IntermediateResults *inResNew = createIntermediateResult();

		inResNew -> tupleAmount = newSize;
		inResNew -> relAmount = templist -> table -> relAmount;

		inResNew -> tupleIDs = malloc((inResNew -> tupleAmount)*sizeof(uint64_t));
		inResNew -> relationIDs = malloc((inResNew -> relAmount)*sizeof(int));
		inResNew -> keys = malloc((inResNew -> relAmount)*sizeof(int32_t*));
		for (int i = 0; i < inResNew -> relAmount; i++)
		{
			inResNew -> keys[i] = malloc((inResNew -> tupleAmount)*sizeof(int32_t));
		}

		for (int i = 0; i < inResNew -> relAmount; i++)
		{
			inResNew -> relationIDs[i] = templist -> table -> relationIDs[i];
		}

		//then for each relation rowID that has a payload that matches the action
		//copy that rowID from the old inres table to the new one 
		int oldTup = 0;
		int newTup = 0;
		for (oldTup = 0; oldTup < rel->num_tuples ; oldTup++)
		{
			if (comparePayloadToValue(rel->tuples[oldTup].payload, value, action))
			{
				for (int relID = 0; relID < inResNew->relAmount ; relID++)
				{
					inResNew -> keys[relID][newTup] = templist -> table -> keys[relID][oldTup];
				}
				newTup++;
			}
		}

		//last, delete the old result table and assign the new one to the list node
		IntermediateResultsDel(templist->table);
		templist->table = inResNew;
	}
	else//relation doesn't exist in an intermediate results table
	{//a new one has to be created and will be filled with a single column
		//the column values will be the result of the comparison
		printf("Relation doesn't exist!\n");

		//first, create relation from table
		relation *rel = createRelationFromTable(t, columnID);

		//then, count how many of the rows meet the action requirement
		int newSize = calculateActionResultAmount(rel, value, action);

		//then, allocate memory for a new intermediate results table
		IntermediateResults *inResNew = createIntermediateResult();

		inResNew -> tupleAmount = newSize;
		inResNew -> relAmount = 1;

		inResNew -> tupleIDs = malloc((newSize)*sizeof(uint64_t));
		inResNew -> relationIDs = malloc((1)*sizeof(int));
		inResNew -> keys = malloc((inResNew -> relAmount)*sizeof(int32_t*));

		for (int i = 0; i < inResNew -> relAmount; i++)
		{
			inResNew -> keys[i] = malloc((inResNew -> tupleAmount)*sizeof(int32_t));
		}

		inResNew -> relationIDs[0] = relationID;

		//then for each relation rowID that has a payload that matches the action
		//copy that rowID from the relation struct to the intermediate result table 
		int relSize = 0;
		int newTup = 0;
		for (relSize = 0; relSize < rel->num_tuples ; relSize++)
		{
			if (comparePayloadToValue(rel->tuples[relSize].payload, value, action))
			{
				//for (int relID = 0; relID < inResNew->relAmount ; relID++)
					inResNew -> keys[0][newTup] = rel->tuples[relSize].key;
				newTup++;
			}
		}

		//last, add the new intermediate result table to the list
		list = addNodeToList(list, inResNew);
	}

	return list;
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

		IntermediateResults* temp = mergeIntermediateResults(head, t, relationA, relationB, columnA, columnB);
		int indexA = getIntermediateResultsSingleIndex(head, relationA);
		int indexB = getIntermediateResultsSingleIndex(head, relationB);
		deleteNodeFromList(head, indexA);
		deleteNodeFromList(head, indexB);
		addNodeToList(head, temp);
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


IntermediateResults* mergeIntermediateResults(IntermediateResultsList* inRes, table** t, int relationA, int relationB, int columnA, int columnB) {

    int indexA = 0, indexB = 0;

    for (int i = 0; i < intermediateResultsAmount; i++) {
        if (existsInIntermediateResults(getNodeFromList(inRes, i), relationA))
            indexA = i;
        if (existsInIntermediateResults(getNodeFromList(inRes, i), relationB))
            indexB = i;
    }

    relation* relA = createRelationFromIntermediateResults(getNodeFromList(inRes, indexA), t[relationA], relationA, columnA);
    relation* relB = createRelationFromIntermediateResults(getNodeFromList(inRes, indexB), t[relationB], relationB, columnB);

    result* results = RadixHashJoin(relA, relB);

    IntermediateResults* r = createIntermediateResult();

    r->relAmount = getNodeFromList(inRes, indexA)->relAmount + getNodeFromList(inRes, indexB)->relAmount;
    r->relationIDs = malloc(r->relAmount*sizeof(int32_t));
    for(int i=0; i<getNodeFromList(inRes, indexA)->relAmount; i++)
    	r->relationIDs[i] = getNodeFromList(inRes, indexA)->relationIDs[i];
    for(int i=getNodeFromList(inRes, indexA)->relAmount; i<r->relAmount; i++)
    	r->relationIDs[i] = getNodeFromList(inRes, indexB)->relationIDs[i - getNodeFromList(inRes, indexA)->relAmount];
    r->tupleAmount = (uint64_t) getResultsAmount();

    r->keys = malloc(r->relAmount*sizeof(int32_t*));
    for(int i=0; i<r->relAmount; i++)
    	r->keys[i] = malloc(r->tupleAmount*sizeof(int32_t));

    for(int j=0; j<r->tupleAmount; j++){

		for(int i=0; i<getNodeFromList(inRes, indexA)->relAmount; i++)
			r->keys[i][j] = getNodeFromList(inRes, indexA)->keys[i][results->results[j%getResultTuplesPerPage()].relation_R];
		for(int i=getNodeFromList(inRes, indexA)->relAmount; i<r->relAmount; i++)
			r->keys[i][j] = getNodeFromList(inRes, indexB)->keys[i - getNodeFromList(inRes, indexA)->relAmount][results->results[j%getResultTuplesPerPage()].relation_S];

    }

    intermediateResultsAmount--;

	return r;
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

int getIntermediateResultsSingleIndex(IntermediateResultsList* inRes, int relationA){

	IntermediateResultsList* curr = inRes->next;

	int index = 0;

	while(curr != NULL){

		if(existsInIntermediateResults(curr->table, relationA))
			return index;
		curr = curr->next;
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

