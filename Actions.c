#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "Actions.h"

void executeQuery(table **t, Query *q)
{
	int actions = q->comparison_set->comparisons_num;
	int* rels = q->query_relation_set->query_relations;

	//calculate action priorities
	calculateActionPriorities(t, q);

	//save comparisons in an array, cmp
	Comparison cmp[actions];
	Comparison temp;
	for (int i = 0; i < actions; i++)
		cmp[i] = q->comparison_set->comparisons[i];

	//sort comparisons according to their priority
	for (int i = 0; i < actions-1; i++)
	{
		for (int j = 0; j < actions -i-1; j++)
		{
			if(cmp[j].priority > cmp[j+1].priority)
			{
				temp = cmp[j];
				cmp[j] = cmp[j+1];
				cmp[j+1] = temp;
			}
		}
	}

	//print actions in increasing priority order
	/*for (int i = 0; i < actions; i++)
	{
		printf("%d.%d", cmp[i].relationA, cmp[i].columnA);
		if(cmp[i].action == LESS_THAN)
			printf(" < ");
		else if(cmp[i].action == GREATER_THAN)
			printf(" > ");
		else
			printf(" = ");

		if(cmp[i].action == JOIN)
			printf("%d.%d ", cmp[i].relationB , cmp[i].columnB);
		else
			printf("%d ", cmp[i].relationB);

		printf("(pr: %d)\n", cmp[i].priority);
	}*/

	//execute actions in query
	IntermediateResultsList* inRes = createList();
	for (int i = 0; i < actions; i++)
	{
		printf("%d.%d", cmp[i].relationA, cmp[i].columnA);
		if(cmp[i].action == LESS_THAN)
			printf(" < ");
		else if(cmp[i].action == GREATER_THAN)
			printf(" > ");
		else
			printf(" = ");

		if(cmp[i].action == JOIN)
			printf("%d.%d ", cmp[i].relationB , cmp[i].columnB);
		else
			printf("%d ", cmp[i].relationB);

		printf("(pr: %f)\n", cmp[i].priority);
		
		if(cmp[i].action != JOIN)
		{
			inRes = compareColumn(inRes, t[rels[cmp[i].relationA]] , rels[cmp[i].relationA], cmp[i].columnA , cmp[i].relationB , cmp[i].action);
		}
		else if(cmp[i].action == JOIN)
		{
			if (cmp[i].relationA == cmp[i].relationB)
			{
				inRes = joinSameRelation(inRes, t, rels[cmp[i].relationA], cmp[i].columnA, cmp[i].columnB);
			}
			else
			{
				inRes = joinRelationsRadix(inRes, t, rels[cmp[i].relationA], rels[cmp[i].relationB], cmp[i].columnA, cmp[i].columnB);
			}
		}
		else
		{
			fprintf(stderr, "wtf\n");
		}

		setResultsAmount(0);
	}

	printActionResults(t, inRes, q->column_set, rels);

	deleteList(&inRes);
}

void printActionResults(table** t, IntermediateResultsList *inRes, Column_t *columns, int* rels) {

    IntermediateResults* result = getIntermediateResultFromColumns(t, inRes, columns, rels);
    int index;

    uint64_t sums[columns->columns_num];
    for(int i=0; i<columns->columns_num; i++)
    {
        printf("(%d.%d)\t", rels[columns->columns[i].relation], columns->columns[i].column);
		sums[i] = 0;    
    }
    printf("\n\n");

    for(int j=0; j<result->tupleAmount; j++){

        for(int i=0; i<columns->columns_num; i++) {

            index = getColumnIntermediateResultsIndex(result, rels[columns->columns[i].relation]);
            //printf("%5lu\t", t[result->relationIDs[index]]->columns[columns->columns[i].column][result->keys[index][j]]);
            sums[i] += t[result->relationIDs[index]]->columns[columns->columns[i].column][result->keys[index][j]];

        }
        //printf("\n");
    }

    for (int i = 0; i < columns->columns_num; i++)
    {
    	if(result->tupleAmount == 0)
    		printf("NULL ");
    	else
    		printf("%lu ", sums[i]);
    }
    printf("\nTotal: %lu\n", result->tupleAmount);
    printf("------------------------------------------------------------\n");

}


void calculateActionPriorities(table **t, Query *q)
{
	Comparison *cmp =NULL;
	uint64_t distinctsA, distinctsB, min, max, tuples, tuplesA, tuplesB;
	for (int action = 0; action < q->comparison_set->comparisons_num; action++)
	{
		cmp =& (q->comparison_set->comparisons[action]);

		min = t[q->query_relation_set->query_relations[cmp->relationA]]->metadata[cmp->columnA].min;
		max = t[q->query_relation_set->query_relations[cmp->relationA]]->metadata[cmp->columnA].max;
		distinctsA = t[q->query_relation_set->query_relations[cmp->relationA]]->metadata[cmp->columnA].distincts;
		tuples = t[q->query_relation_set->query_relations[cmp->relationA]] -> size;
		tuplesA = tuples;

		if(cmp->action == JOIN)
		{
			distinctsB = t[q->query_relation_set->query_relations[cmp->relationB]]->metadata[cmp->columnB].distincts;
			tuplesB = t[q->query_relation_set->query_relations[cmp->relationB]] -> size;

			if(cmp->relationA == cmp->relationB)//same relation
			{
				cmp->priority = 1.0;
			}
			else if(q->query_relation_set->query_relations[cmp->relationA] == q->query_relation_set->query_relations[cmp->relationB])
			{//self join!
				if(cmp->columnA == cmp->columnB)
					cmp->priority = (float) ((tuples -distinctsA) * (tuples -distinctsA) + distinctsA);
				else if(distinctsA < distinctsB)
					cmp->priority = (float) (tuplesA);
				else
					cmp->priority = (float) (tuplesB);

			}
			else
			{	
				distinctsA = t[q->query_relation_set->query_relations[cmp->relationA]]->metadata[cmp->columnA].distincts;
				distinctsB = t[q->query_relation_set->query_relations[cmp->relationB]]->metadata[cmp->columnB].distincts;
				if(distinctsA < distinctsB)
					cmp->priority = (float) tuplesA;
				else
					cmp->priority = (float) tuplesB;
				//cmp->priority = distinctsA + distinctsB;
			}
		}
		else if(cmp->action == EQUAL)
		{
			//distinctsA = t[q->query_relation_set->query_relations[cmp->relationA]]->metadata[cmp->columnA].distincts;
			cmp->priority = 0.0;//(float) distinctsA;
		}
		else if (cmp->action == LESS_THAN)
		{
			int value = cmp->relationB;
			if (value < min)//no value less than minimum
				cmp->priority = 0.0;//do first and return NULL
			else if(value >= max)
				cmp->priority = (float) tuples;
			else
			{
				float percentage = ((float) (value - min)) / ((float) (max - min));
				cmp->priority = (float) tuples * percentage;
			}
		}
		else if (cmp->action == GREATER_THAN)
		{
			int value = cmp->relationB;

			if (value > max)//no value greater than max
				cmp->priority = 0.0;//do first
			else if (value <= min)
				cmp->priority = (float) tuples;
			else
			{
				float percentage = ((float) (max - value)) / ((float) (max - min));
				cmp->priority = (float) tuples * percentage;
			}
			
		}
	}
}

IntermediateResults *getIntermediateResultFromColumns(table** t, IntermediateResultsList *inRes, Column_t *columns, int* rels) {

    if(columns->columns_num == 0)
        return NULL;

    int index = getIntermediateResultsSingleIndex(inRes, rels[columns->columns[0].relation]);
    IntermediateResults* result;

    if(index != -1)
        result = getNodeFromList(inRes, index);
    else
        result = createIntermediateResultFromTable(t[rels[columns->columns[0].relation]], rels[columns->columns[0].relation]);


    for(int i=1; i<columns->columns_num; i++){

        if(existsInIntermediateResults(result, rels[columns->columns[i].relation]))
            continue;

        index = getIntermediateResultsSingleIndex(inRes, rels[columns->columns[i].relation]);

        if(index != -1)
            result = crossProductIntermediateResults(result, getNodeFromList(inRes, index));
        else {
            IntermediateResults *temp = createIntermediateResultFromTable(t[rels[columns->columns[i].relation]], rels[columns->columns[i].relation]);
            result = crossProductIntermediateResults(result, temp);
            IntermediateResultsDel(temp);
        }
    }

    return result;
}

IntermediateResults *crossProductIntermediateResults(IntermediateResults *inResA, IntermediateResults *inResB) {

    IntermediateResults* result = createIntermediateResult();
    IntermediateResultsAlloc(&result, (inResA->tupleAmount *  inResB->tupleAmount) , (inResA->relAmount + inResB->relAmount));

    for(int i=0; i<inResA->relAmount; i++)
        result->relationIDs[i] = inResA->relationIDs[i];
    for(int i=0; i<inResB->relAmount; i++)
        result->relationIDs[inResA->relAmount + i] = inResB->relationIDs[i];

    for(int i=0; i<inResA->tupleAmount; i++){

        for(int j=0; j<inResB->tupleAmount; j++){

            for(int k=0; k<inResA->relAmount; k++)
                result->keys[k][(i*inResB->tupleAmount) + j] = inResA->keys[k][i];
            for(int k=0; k<inResB->relAmount; k++)
                result->keys[k+inResA->relAmount][(i*inResB->tupleAmount) + j] = inResB->keys[k][j];

        }

    }

    IntermediateResultsDel(inResA);

    return result;
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

void IntermediateResultsAlloc(IntermediateResults **inResaddr, uint64_t tupleAmount, int relAmount)
{
	if((*inResaddr) == NULL)
		IntermediateResultsInit(inResaddr);

	IntermediateResults *inRes = *inResaddr;

	inRes -> tupleAmount = tupleAmount;
	inRes -> relAmount = relAmount;

	inRes -> tupleIDs = malloc(tupleAmount*sizeof(uint64_t));
	inRes -> relationIDs = malloc(relAmount*sizeof(int));

	inRes -> keys = malloc(relAmount * sizeof(int32_t*));
	for (int i = 0; i < relAmount; i++)
	{
		inRes -> keys[i] = malloc(tupleAmount * sizeof(int32_t));
	}
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
    for(int i=0; i<inRes->relAmount; i++)
        free(inRes->keys[i]);
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

int calculateSameJoinResultsAmount(relation* relA, relation* relB){

	int counter = 0;
	for(int i=0; i<relA->num_tuples; i++)
		if(relA->tuples[i].payload == relB->tuples[i].payload) {
			//printf("%d - %d\n", relA->tuples[i].key, relA->tuples[i].payload);
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
	//printf("%d.%d (%d) %d\n", relationID, columnID, action, value);

	IntermediateResultsList* templist = list->next;
	int cnt = intermediateResultsAmount;


	while((templist != NULL)&&(cnt))
	{
		if(existsInIntermediateResults(templist->table, relationID))
			break;
		templist = templist -> next;
		cnt--;
	}

	if(templist != NULL && cnt > 0)//relation exists in an intermediate results table
	{//we need to remove the tuples that do not match the comparison result
		//printf("Relation exists!\n");
		//first, create a relation from the existing results
		//each key of the relation will be the tupleID of the intermediate result tuple
		relation *rel = createRelationFromIntermediateResults(templist->table, t, relationID, columnID);

		//then, count how many of the rows meet the action requirement
		int newSize = calculateActionResultAmount(rel, value, action);

		//then, create a new Intermediate Results table with newSize tuples
		IntermediateResults *inResNew = createIntermediateResult();
		IntermediateResultsAlloc(&inResNew, newSize, templist->table->relAmount);


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
		//printf("Relation doesn't exist!\n");

		//first, create relation from table
		relation *rel = createRelationFromTable(t, columnID);

		//then, count how many of the rows meet the action requirement
		int newSize = calculateActionResultAmount(rel, value, action);

		//then, allocate memory for a new intermediate results table
		IntermediateResults *inResNew = createIntermediateResult();
		IntermediateResultsAlloc(&inResNew, newSize, 1);

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
		addNodeToList(list, inResNew);

		freeRelation(rel);
	}

	return list;
}


IntermediateResultsList* joinSameRelation(IntermediateResultsList* head, table **t, int relationA, int columnA, int columnB) {

	int index = getIntermediateResultsSingleIndex(head, relationA);

	relation* relA, *relB;

	if(index != -1){

		relA = createRelationFromIntermediateResults(getNodeFromList(head, index), t[relationA], relationA, columnA);
		relB = createRelationFromIntermediateResults(getNodeFromList(head, index), t[relationA], relationA, columnB);

	}
	else{

		relA = createRelationFromTable(t[relationA], columnA);
		relB = createRelationFromTable(t[relationA], columnB);

	}

	int count = calculateSameJoinResultsAmount(relA, relB);

	IntermediateResults* inRes = createIntermediateResult();

	inRes->tupleAmount = (uint64_t) count;
	inRes->relAmount = index == -1 ? 1 : getNodeFromList(head, index)->relAmount;
	inRes->relationIDs = malloc(inRes->relAmount*sizeof(int32_t));
	if(index == -1)
		inRes->relationIDs[0] = relationA;
	else
		for(int i=0; i<inRes->relAmount; i++)
			inRes->relationIDs[i] = getNodeFromList(head, index)->relationIDs[i];

	inRes->keys = malloc(inRes->relAmount*sizeof(int32_t*));
	for(int i=0; i<inRes->relAmount; i++)
		inRes->keys[i] = malloc(inRes->tupleAmount*sizeof(int32_t));

	int tupleCounter = 0;
	for(int i=0; i<relA->num_tuples; i++){

		if(relA->tuples[i].payload != relB->tuples[i].payload)
			continue;

		for(int j=0; j<inRes->relAmount; j++){

			if(index == -1)
				inRes->keys[j][tupleCounter] = relA->tuples[i].key;
			else
				inRes->keys[j][tupleCounter] = getNodeFromList(head, index)->keys[j][i];

		}

		tupleCounter++;
	}

	if(index != -1)
		deleteNodeFromList(head, index);

	addNodeToList(head, inRes);

	return head;
}


IntermediateResultsList* joinRelationsRadix(IntermediateResultsList* head, table **t, int relationA, int relationB, int columnA, int columnB){

	IntermediateResultsList* node;

	int cat = getQueryCategory(head, relationA, relationB);
	int index = 0;

	if(cat == 0){											//	No intermediate results table has any of the two
															// 	relations
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
		freeRelation(relA);
		freeRelation(relB);
		freeResults(results);
		return head;
	}

	/**
	 * Fill the newly created empty IntermediateResults table.
	 */
	IntermediateResults* temp = addResultToNewIntermediateResult(results, createIntermediateResult(), relationA, relationB);

	addNodeToList(head, temp);

	freeResults(results);
    freeRelation(relA);
    freeRelation(relB);

	return head;

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
	IntermediateResultsAlloc(&r, (uint64_t) getResultsAmount(), (getNodeFromList(inRes,indexA)->relAmount + getNodeFromList(inRes,indexB)->relAmount) );

    for(int i=0; i<getNodeFromList(inRes, indexA)->relAmount; i++)
    	r->relationIDs[i] = getNodeFromList(inRes, indexA)->relationIDs[i];
    for(int i=getNodeFromList(inRes, indexA)->relAmount; i<r->relAmount; i++)
    	r->relationIDs[i] = getNodeFromList(inRes, indexB)->relationIDs[i - getNodeFromList(inRes, indexA)->relAmount];
    

    for(int j=0; j<r->tupleAmount; j++){

		for(int i=0; i<getNodeFromList(inRes, indexA)->relAmount; i++)
			r->keys[i][j] = getNodeFromList(inRes, indexA)->keys[i][results->results[j%getResultTuplesPerPage()].relation_R];
		for(int i=getNodeFromList(inRes, indexA)->relAmount; i<r->relAmount; i++)
			r->keys[i][j] = getNodeFromList(inRes, indexB)->keys[i - getNodeFromList(inRes, indexA)->relAmount][results->results[j%getResultTuplesPerPage()].relation_S];

		if(j%getResultTuplesPerPage() == 0 && j!=0)
		    results = results->next;
    }

    freeResults(results);
    freeRelation(relA);
    freeRelation(relB);
    
	return r;
}


IntermediateResults* addResultToNewIntermediateResult(result *results, IntermediateResults *inRes, int relationA, int relationB) {

	IntermediateResultsAlloc(&inRes, (uint64_t) getResultsAmount(), 2);

	inRes->relationIDs[0] = relationA;
	inRes->relationIDs[1] = relationB;
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

	//IntermediateResultsDel(inRes);

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

	//IntermediateResultsDel(inRes);

	return r;
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

    if(index >= intermediateResultsAmount)
        return NULL;

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

void deleteNodeFromList(IntermediateResultsList* head, int index) {

	if ((head->next == NULL) || (index<0) ||(intermediateResultsAmount<=0))
	{
		fprintf(stderr, "deleteNodeFromList(): invalid arguments\n");
		return;
	}

	IntermediateResultsList* curr = head->next;
	IntermediateResultsList* prev = head;

	while(index) {
		if(curr==NULL)
		{
			fprintf(stderr, "deleteNodeFromList(): invalid index\n");
			return;
		}
		prev = curr;
		curr = curr->next;
		index--;
	}

	prev->next = curr->next;
	IntermediateResultsDel(curr->table);
	free(curr);
	intermediateResultsAmount--;
}


void deleteList(IntermediateResultsList** headaddr)
{
	IntermediateResultsList* head = *headaddr;
	IntermediateResultsList* temp = NULL;

	while(head->next!=NULL)
	{
		temp = head->next;
		head->next = temp->next;
		IntermediateResultsDel(temp->table);
		free(temp);
		intermediateResultsAmount--;
	}

	//IntermediateResultsDel(&(head->table));
	free(head->table);
	free(head);
	(*headaddr) = NULL;
}

IntermediateResults* createIntermediateResult(){

	IntermediateResults* table;
	IntermediateResultsInit(&table);

	return table;
}


IntermediateResults* createIntermediateResultFromTable(table* t, int relation){

    IntermediateResults* result = createIntermediateResult();
    IntermediateResultsAlloc(&result, t->size, 1);

    result->relationIDs[0] = relation;

    for(int i=0; i<result->tupleAmount; i++)
        result->keys[0][i] = i;

    return result;

}


int getColumnIntermediateResultsIndex(IntermediateResults* inRes, int relation){

    for(int i=0; i<inRes->relAmount; i++)
        if(inRes->relationIDs[i] == relation)
            return i;

    return -1;

}

