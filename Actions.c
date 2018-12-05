#include <stdio.h>
#include <stdlib.h>


#include "Actions.h"


void executeQueries(table **t, Query *q)
{
	//parseTableData(t);
	result *temp = NULL;
	int tableID1 = -1;
	int tableID2 = -1;
	relation * rel1 = NULL;
	relation * rel2 = NULL;

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
				/* code */
			}
			else
			{
				//radix
			}
		}
		else
		{
			fprintf(stderr, "wtf\n");
		}
	}
}

void compareColumn(table *t , int colA , int value , int action)
{
	fprintf(stdout, "%d %d %d\n", colA, value, action);
	int counter = 0;
	uint64_t keytemp;
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
	t->inRes->amount = counter;

	for (int i = 0; i < counter; i++)
	{
		printf("%d\n", t->inRes->keys[i]);
	}
	printf("%d\n", t->inRes->amount );
}
