#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include "DataGenerator.h"

relation* create_relation_R(){

    int i;
    int maxValue = (int) MAX_VALUE;
    relation* relR = malloc(sizeof(relation));

    srand((unsigned int) time(0));

    relR->num_tuples = (uint32_t) RELR_SIZE;
    relR->tuples = malloc(relR->num_tuples * sizeof(tuple));

    for(i=0; i<relR->num_tuples; i++){

        relR->tuples[i].key = (int32_t ) i;
        relR->tuples[i].payload = rand() % maxValue + 1;

    }

    return relR;
}


relation* create_relation_S(){

    int i;
    int maxValue = (int) MAX_VALUE;
    relation* relS = malloc(sizeof(relation));

    sleep(1);       // Needs this otherwise parallel execution of threads creates same relations R and S

    srand((unsigned int) time(0));

    relS->num_tuples = (uint32_t) RELS_SIZE;
    relS->tuples = malloc(relS->num_tuples * sizeof(tuple));

    for(i=0; i<relS->num_tuples; i++){

        relS->tuples[i].key = (int32_t ) i;
        relS->tuples[i].payload = rand() % maxValue + 1;

    }

    return relS;
}

void print_to_file_R(){



    FILE* file = fopen(relR_name, "w");

    relation* relR = create_relation_R();

    for(int i=0; i<relR->num_tuples; i++){

        fprintf(file, "%d\n", relR->tuples[i].payload);

    }

    fclose(file);
}

void print_to_file_S(){

    FILE* file = fopen(relS_name, "w");

    relation* relS = create_relation_S();

    for(int i=0; i<relS->num_tuples; i++){

        fprintf(file, "%d\n", relS->tuples[i].payload);

    }

    fclose(file);
}


int main(void){

    print_to_file_R();
    print_to_file_S();

}

