#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include "DataGenerator.h"

#define RANDOM 0
#define INCREASING 1
#define IDENTICAL 2
#define EXCLUSIVE 3

int data_flag;

relation* create_relation_R(){

    int i;
    int maxValue = (int) MAX_VALUE;
    relation* relR = malloc(sizeof(relation));

    srand((unsigned int) time(0));

    relR->num_tuples = (uint32_t) RELR_SIZE;
    relR->tuples = malloc(relR->num_tuples * sizeof(tuple));

    for(i=0; i<relR->num_tuples; i++){

        relR->tuples[i].key = (int32_t ) i;
        if (data_flag == RANDOM)
        {
            relR->tuples[i].payload = rand() % MAX_VALUE;
        }
        else if (data_flag == INCREASING)
        {
            relR->tuples[i].payload = i;
        }
        else if (data_flag == IDENTICAL)
        {
            relR->tuples[i].payload = 1;
        }
        else if (data_flag == EXCLUSIVE)
        {
            relR->tuples[i].payload = 1;
        }
        else
        {
            relR->tuples[i].payload = 0;
        }

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
        if (data_flag == RANDOM)
        {
            relS->tuples[i].payload = rand() % MAX_VALUE;
        }
        else if (data_flag == INCREASING)
        {
            relS->tuples[i].payload = i;
        }
        else if (data_flag == IDENTICAL)
        {
            relS->tuples[i].payload = 1;
        }
        else if (data_flag == EXCLUSIVE)
        {
            relS->tuples[i].payload = 2;
        }
        else
        {
            relS->tuples[i].payload = 0;
        }

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


int main(int argc, char **argv){

    if (argc==1)
    {
        fprintf(stderr, "Random payloads\n");
        data_flag = RANDOM;
    }
    else if ((strncmp(argv[1], "1", sizeof(char)))==0)
    {
        fprintf(stderr, "Increasing payloads\n");
        data_flag = INCREASING;
    }
    else if ((strncmp(argv[1], "2", sizeof(char)))==0)
    {
        fprintf(stderr, "Identical payloads\n");
        data_flag = IDENTICAL;
    }
    else if ((strncmp(argv[1], "3", sizeof(char)))==0)
    {
        fprintf(stderr, "Exclusive per relation payloads\n");
        data_flag = EXCLUSIVE;
    }
    else
    {
        fprintf(stderr, "Random payloads\n");
        data_flag = RANDOM;
    }
    print_to_file_R();
    print_to_file_S();

}

