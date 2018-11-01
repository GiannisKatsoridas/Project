#include <stdio.h>
#include <stdlib.h>
#include "Results.h"

result* create_results_page(){

    tuples_per_page = page_size / sizeof(s_tuple);

    result* res = malloc(sizeof(result));

    res->next = NULL;
    res->results = malloc(tuples_per_page * sizeof(s_tuple));

    return res;
}

int add_result(result* res, int32_t value_R, int32_t value_S){

    static int results_num = 0;

    result* pointer = res;
    int bucket = results_num / tuples_per_page;
    int pos = results_num % tuples_per_page;

    if(pos == 0)
        bucket--;

    while(bucket > 0){

        pointer = pointer->next;
        bucket--;

    }

    if(pos == 0 && results_num != 0){

        pointer->next = create_results_page();
        pointer = pointer->next;

    }

    pointer->results[pos].relation_R = value_R;
    pointer->results[pos].relation_S = value_S;

    results_num++;

    return results_num;
}

void print_results(result *res, int results_num) {

    if ((res==NULL) || (results_num==0))
    {
        return;
    }
    result* pointer = res;
    int i;

    while(1){

        for(i=0; i<tuples_per_page; i++){

            printf("%d - %d\n", pointer->results[i].relation_R, pointer->results[i].relation_S);

            results_num--;

            if(results_num == 0)
                return;

        }

        pointer = pointer->next;

    }

}


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