#include <stdio.h>
#include <stdlib.h>
#include "Results.h"


void setTuplesPerPage()
{
	tuples_per_page = page_size / sizeof(s_tuple);
}

int getResultTuplesPerPage()
{
    return tuples_per_page;
}

result* create_results_page()
{
    result* res = malloc(sizeof(result));

    res->next = NULL;
    res->results = malloc(tuples_per_page * sizeof(s_tuple));

    return res;
}

resultsWithNum* create_resultsWithNum(){
	resultsWithNum* x = malloc(sizeof(resultsWithNum));
	x->results_amount = 0;
	x->results = NULL;

	return x;
}

void add_result(resultsWithNum* res, int32_t value_R, int32_t value_S){

    //static int results_num = 0;

    result* pointer = NULL;

    if(res->results == NULL)
    	res->results = create_results_page();
    pointer = res->results;

    int resultnode = res->results_amount / getResultTuplesPerPage();
    int pos = res->results_amount % getResultTuplesPerPage();

    if(pos == 0)
        resultnode--;

    while(resultnode > 0){

        pointer = pointer->next;
        resultnode--;

    }

    if(pos == 0 && res->results_amount != 0){

        pointer->next = create_results_page();
        pointer = pointer->next;

    }

    pointer->results[pos].relation_R = value_R;
    pointer->results[pos].relation_S = value_S;

    res->results_amount++;
}

/*
void print_results(result *res) {

    int res_am = ;

    if ((res==NULL) || (res_am==0))
    {
        return;
    }
    result* pointer = res;
    int i;

    while(1){

        for(i=0; i<tuples_per_page; i++){

            printf("%d - %d\n", pointer->results[i].relation_R, pointer->results[i].relation_S);

            res_am--;

            if(res_am == 0)
                return;

        }

        pointer = pointer->next;

    }

}
*/


unsigned int int_to_int(unsigned int k) {
//function created by HalosGhost
//used to create the binary form of the integer k
//https://stackoverflow.com/questions/5488377/converting-an-integer-to-binary-in-c
    return (k == 0 || k == 1 ? k : ((k % 2) + 10 * int_to_int(k / 2)));
}

void print_relation(relation *rel, FILE *fp)
{
    if ((rel==NULL) || (fp==NULL))
    {
        fprintf(stderr, "relation_print:NULL argument\n");
        return;
    }
    for (int i = 0; i < rel->num_tuples; i++)
    {
        unsigned int binary_payload = int_to_int(rel->tuples[i].payload);
        fprintf(fp, "%4d - %4d (%08d)\n", rel->tuples[i].key, rel->tuples[i].payload, binary_payload);
    }
    fprintf(fp, "\n");
}

void freeResultsWithNum(resultsWithNum* results){

	if(results==NULL)
		return;

    freeResults(results->results);

    free(results);

}

void freeResults(result *results) {

	if(results==NULL)
		return;

    if(results->next!=NULL){
        freeResults(results->next);
    }

    free(results->results);

    free(results);

}


void concatResults(resultsWithNum *res1, resultsWithNum *res2) 
{
	if ((res1==NULL) || (res2==NULL))
	{
		fprintf(stderr, "concatResults(): NULL arguments\n");
		return;
	}

	if((res2->results_amount == 0) || (res2->results == NULL))
		return;

	//STEP 1: 
	//keep address of last and prelast node of results list 1
	//keep address of last node of results list 2

	result *prelast1 = NULL;
	result *last1 = res1->results;
	result *last2 = res2->results;

	while(last1 != NULL)
	{
		if (last1->next != NULL)
		{
			prelast1 = last1;
			last1 = last1->next;
		}
		else
			break;
	}

	while(last2->next != NULL)
	{
		last2 = last2->next;
	}
	//(pre1, l1, l2) = 
	//(NULL, NULL, val)	(result list 1 hasn't got any nodes)
	//or (NULL, val, val)	(result list 1 has only 1 node)
	//or (val, val, val)	(lesult list 1 has more than 1 node)


	//STEP 2:
	//put (the head of) the 2nd list right after :
	//(a)the pre-last node of the 1st list OR
	//(b) the last node of the 1st list, in case that node has EXACTLTY tuples_per_page tuples
	//(to the prelast1->next pointer)
	//update the listnode amount of the first list
	//and discard the second list

	int remaining_tuples = 0;//keeps the tuple amount of the last non-full page in the result list
	if((prelast1 == NULL) && (last1==NULL))
	{//if result list 1 is empty, just save data from list 2 to list 1
		res1->results = res2->results;
		res1->results_amount = res2->results_amount;

		res2->results = NULL;
		res2->results_amount = 0;
		freeResultsWithNum(res2);
		return;
	}
	else if(prelast1==NULL)
	{//else if result list 1 has 1 node
		remaining_tuples = res1->results_amount % getResultTuplesPerPage();
		if (remaining_tuples == 0)
		{//if last (and only) node in result list 1 has EXACTLY tuples_per_page tuples
			last1->next = res2->results;
			res1->results_amount += res2->results_amount;
			last1 = NULL;
		}
		else
		{
			res1->results = res2->results;
			res1->results_amount = res2->results_amount;
		}

		res2->results = NULL;
		res2->results_amount = 0;
		freeResultsWithNum(res2);
	}
	else
	{//else if result list 1 has more than 1 nodes
		remaining_tuples = res1->results_amount % getResultTuplesPerPage();
		if (remaining_tuples == 0)
		{//if last node in result list 1 has EXACTLY tuples_per_page tuples
			last1->next = res2->results;
			res1->results_amount += res2->results_amount;
			last1 = NULL;
		}
		else
		{
			prelast1->next = res2->results;
			res1->results_amount = res1->results_amount - remaining_tuples + res2->results_amount;
		}

		res2->results = NULL;
		res2->results_amount = 0;
		freeResultsWithNum(res2);
	}

	//STEP 3:
	//add result tuples from last1 node to the end of last2 node
	//(remember: the last2 node is now at the end of the 1st list)
	//and discard the last1 node after it is copied
	for (int i = 0; i < remaining_tuples; i++)
	{
		add_result(res1, last1->results[i].relation_R, last1->results[i].relation_S);
	}

	freeResults(last1);
}
