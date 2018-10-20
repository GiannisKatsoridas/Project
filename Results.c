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

