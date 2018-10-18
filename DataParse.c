#include <stdio.h>
#include <stdlib.h>
#include "DataParse.h"


void set_relation_R() {

    char* line = NULL;
    size_t length;
    int size;

    FILE* file = fopen(relR_name, "r");

    relation_R = malloc(sizeof(relation));
    relation_R->num_tuples = RELR_SIZE;
    relation_R->tuples = malloc(relation_R->num_tuples * sizeof(tuple));

    for(int i=0; i<relation_R->num_tuples; i++){

        size = getline(&line, &length, file);

        relation_R->tuples[i].key = i;
        relation_R->tuples[i].payload = (int32_t) atoi(line);

        free(line);
        line = NULL;

    }

    fclose(file);
}

void set_relation_S() {

    char* line = NULL;
    size_t length;

    FILE* file = fopen(relS_name, "r");

    relation_S = malloc(sizeof(relation));
    relation_S->num_tuples = RELS_SIZE;
    relation_S->tuples = malloc(relation_S->num_tuples * sizeof(tuple));

    for(int i=0; i<relation_S->num_tuples; i++){

        getline(&line, &length, file);

        relation_S->tuples[i].key = i;
        relation_S->tuples[i].payload = (int32_t) atoi(line);

        free(line);
        line = NULL;

    }

    fclose(file);
}

int find_suffix(){

    int suffix1 = find_suffix_R();
    int suffix2 = find_suffix_S();

    return suffix1 > suffix2? suffix1 : suffix2;
}

int find_suffix_R(){

    

}

int find_suffix_S() {



}
