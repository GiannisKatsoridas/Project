#include <stdio.h>
#include <stdlib.h>
#include "DataParse.h"

relation* get_relation(char* name, int size){

    char* line = NULL;
    size_t length;

    FILE* file = fopen(name, "r");
    if (file==NULL)
    {
        fprintf(stderr, "get_relation:fopen()\n");
        exit(-1);
    }

    relation* rel = malloc(sizeof(relation));
    rel->num_tuples = (uint32_t ) size;
    rel->tuples = malloc(rel->num_tuples * sizeof(tuple));

    for(int i=0; i<rel->num_tuples; i++){

        getline(&line, &length, file);

        rel->tuples[i].key = i;
        rel->tuples[i].payload = (int32_t) atoi(line);

    }
    free(line);
    line = NULL;
    fclose(file);

    return rel;

}

/*relation* get_relation_from_table(table *t, int columnID)
{
    if ((t==NULL) || (columnID < 0))
    {
        printf("get_relation_from_table(): NULL table\n");
        return NULL;
    }

    if ((columnID > (t->size-1) ) || (columnID < 0))
    {
        printf("get_relation_from_table(): invalid columnID %d\n", columnID);
        return NULL;
    }

    //create relation
    relation* rel = malloc(sizeof(relation));
    rel->num_tuples = t -> size;

    //assign to the relation tuples the payloads from the columnID of the table
    rel->tuples = malloc(rel->num_tuples * sizeof(tuple));
    for(int i=0; i<rel->num_tuples; i++)
    {
        rel->tuples[i].key = i;
        rel->tuples[i].payload = t->columns[columnID][i];
    }
}*/

int* create_histogram(relation* rel){

    int i, buckets, pos;
    int suffix = RADIX_N;

    buckets = power_of_2(suffix);

    int* histogram = malloc(buckets * sizeof(int));

    for(i=0; i<buckets; i++){
        histogram[i] = 0;
    }

    for(i=0; i<rel->num_tuples; i++){

        pos = rel->tuples[i].payload % buckets;

        histogram[pos]++;


    }

    return histogram;
}

int power_of_2(int power){

    int sum=1;

    for(int i=0; i<power; i++){

        sum *= 2;

    }

    return sum;
}


int* create_psum(int* histogram, int size) {

    int i, sum = 0;
    int* psum = malloc(size * sizeof(int));

    for(i=0; i<size; i++){

        psum[i] = sum;
        sum += histogram[i];

    }

    return psum;
}


relation *create_relation_new(relation *rel, int *psum, int buckets) {

    int i, pos;

    relation* relation_new = malloc(sizeof(relation));

    relation_new->num_tuples = rel->num_tuples;
    relation_new->tuples = malloc(rel->num_tuples * sizeof(tuple));

    for(i=0; i<relation_new->num_tuples; i++){

        pos = psum[rel->tuples[i].payload % buckets];

        relation_new->tuples[pos].key = rel->tuples[i].key;
        relation_new->tuples[pos].payload = rel->tuples[i].payload;

        psum[rel->tuples[i].payload % buckets]++;
    }

    return  relation_new;
}

void freeRelation(relation *rel)
{
    free(rel->tuples);
    free(rel);
}

int *initializeHistogram(int size) {

    int* histogram = malloc(size * sizeof(int));
    for(int i=0; i<size; i++)
        histogram[i] = 0;

    return histogram;
}
