#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdbool.h>
#include "JoinEnumeration.h"
#include "Tables.h"

void freeJEStats(JEStats *jes);

int **createGraph(Query *query) {

    int** graph = malloc(query->query_relation_set->query_relations_num * sizeof(int*));
    for(int i=0; i<query->query_relation_set->query_relations_num; i++){

        graph[i] = malloc(query->query_relation_set->query_relations_num * sizeof(int));
        for(int j=0; j<query->query_relation_set->query_relations_num; j++){
            graph[i][j] = 0;
        }

    }

    for(int i=0; i<query->comparison_set->comparisons_num; i++){

        if(query->comparison_set->comparisons[i].action == JOIN){

            graph[query->comparison_set->comparisons[i].relationA][query->comparison_set->comparisons[i].relationB] = 1;
            graph[query->comparison_set->comparisons[i].relationB][query->comparison_set->comparisons[i].relationA] = 1;

        }

    }

    return graph;
}

int isConnected(int **graph, int newRelation, int *relations, int relationsNum) {

    for(int i=0; i<relationsNum; i++){

        if(graph[newRelation][relations[i]] == 1)
            return 1;

    }

    return 0;

}

void permute(int *array, int i, int length, PermutationsQueue* pq) {
    if (length == i){
        int* arr = malloc(length * sizeof(int));
        for(int k=0; k<length; k++)
            arr[k] = array[k];
        PQPush(pq, arr);
        return;
    }
    int j;
    for (j = i; j < length; j++) {
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
        permute(array,i+1,length, pq);
        temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
}

PermutationsQueue* PQInit(){

    PermutationsQueue* head = malloc(sizeof(PermutationsQueue));
    head->next = NULL;
    head->array = NULL;

    return head;

}

void PQPush(PermutationsQueue* pq, int* array){

    PermutationsQueue* node = malloc(sizeof(PermutationsQueue));
    node->array = array;
    node->next = NULL;

    PermutationsQueue* pointer = pq;
    while(pointer->next != NULL)
        pointer = pointer->next;
    pointer->next = node;

}

int* PQPop(PermutationsQueue* pq){

    PermutationsQueue* node = pq->next;
    if(node == NULL)
        return NULL;
    PermutationsQueue* next = node->next;
    pq->next = next;

    int* arr = node->array;

    free(node);

    return arr;
}

void PQDelete(PermutationsQueue* pq){

    while(pq->next != NULL){

        PermutationsQueue* node = pq->next;
        if(pq->array != NULL)
            free(pq->array);
        free(pq);
        pq = node;

    }

    free(pq);

}

int hashFunctionBestTree(int *array, int length) {

    int result = 0;

    for(int i=0; i<length; i++){

        result += factorial(array[i] + 1);

    }

    return result;

}

int factorial(int a){

    if(a == 1 || a == 0)
        return a;

    return (factorial(a-1)*a);

}

int *createArray(int length) {

    int* result = malloc(length * sizeof(int));
    for(int i=0; i<length; i++)
        result[i] = i;

    return result;

}

void getCombination(int arr[], int n, int r, PermutationsQueue* pq)
{
    int data[r];

    combinationUtil(arr, data, 0, n-1, 0, r, pq);
}

void combinationUtil(int arr[], int data[], int start, int end, int index, int r, PermutationsQueue* pq)
{
    if (index == r)
    {
        permute(data, 0, r, pq);
        return;
    }

    for (int i=start; i<=end && end-i+1 >= r-index; i++)
    {
        data[index] = arr[i];
        combinationUtil(arr, data, i+1, end, index+1, r, pq);
    }
}

JEStats* initializeJEStats(table** t, Query* q){

    JEStats* jes = malloc(sizeof(JEStats));

    jes->relations_num = q->query_relation_set->query_relations_num;
    jes->intermediateResults = malloc(sizeof(int));
    jes->intermediateResultsNum = 1;
    jes->results_num = 0;
    jes->jesbr = malloc(q->query_relation_set->query_relations_num * sizeof(JEStatsByRelation));

    for(int i=0; i<q->query_relation_set->query_relations_num; i++){

        jes->jesbr[i].columns_num = (int) t[q->query_relation_set->query_relations[i]]->columns_size;
        jes->jesbr[i].jesbc = malloc(t[q->query_relation_set->query_relations[i]]->columns_size * sizeof(JEStatsByColumn));

        for(int j=0; j<t[q->query_relation_set->query_relations[i]]->columns_size; j++){

            jes->jesbr[i].jesbc[j].u = (int) t[q->query_relation_set->query_relations[i]]->metadata[j].max;
            jes->jesbr[i].jesbc[j].l = (int) t[q->query_relation_set->query_relations[i]]->metadata[j].min;
            jes->jesbr[i].jesbc[j].f = (int) t[q->query_relation_set->query_relations[i]]->size;
            jes->jesbr[i].jesbc[j].d = t[q->query_relation_set->query_relations[i]]->metadata[j].distincts;

        }

    }

    return jes;

}

void applyInitialFilters(JEStats *jes, Query *q, table** t) {

    for(int i=0; i<q->comparison_set->comparisons_num; i++){

        if(q->comparison_set->comparisons[i].action != JOIN)

            switch (q->comparison_set->comparisons[i].action){

                case GREATER_THAN:
                    executeGreaterThanFilter(jes, q->comparison_set->comparisons[i], t, q->query_relation_set->query_relations);
                    break;

                case LESS_THAN:
                    executeLessThanFilter(jes, q->comparison_set->comparisons[i], t, q->query_relation_set->query_relations);
                    break;

                case EQUAL:
                    executeEqualFilter(jes, q->comparison_set->comparisons[i], t, q->query_relation_set->query_relations);
                    break;

                default:
                    fprintf(stderr, "Wrong action in the query.\n");
                    break;

            }

        else if(q->comparison_set->comparisons[i].columnA == q->comparison_set->comparisons[i].columnB)
            executeSameRelationFilter(jes, q->comparison_set->comparisons[i], t, q->query_relation_set->query_relations);

    }

}

void executeSameRelationFilter(JEStats *jes, Comparison comparison, table **t, int* relations) {

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l =
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l > jes->jesbr[comparison.relationB].jesbc[comparison.columnB].l ?
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l :
            jes->jesbr[comparison.relationB].jesbc[comparison.columnB].l;
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].l = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u =
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u < jes->jesbr[comparison.relationB].jesbc[comparison.columnB].u ?
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u :
            jes->jesbr[comparison.relationB].jesbc[comparison.columnB].u;
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].u = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u;


    int n = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l + 1;
    int fA = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f = (int) (
            (double) jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f / n);
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d = (int) (
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d *
            calculateStatsDistinctsFactor(jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f,
                                              fA,
                                              fA,
                                              jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d));
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].d = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d;

    for(int i=0; i<t[relations[comparison.relationA]]->columns_size; i++){

        if(i==comparison.columnA || i==comparison.columnB)
            continue;

        jes->jesbr[comparison.relationA].jesbc[i].d = (int) (
                jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d *
                calculateStatsDistinctsFactor(jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].d));
        jes->jesbr[comparison.relationA].jesbc[i].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    }

    jes->results_num = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;
}

void executeEqualFilter(JEStats *jes, Comparison comparison, table** t, int* relations) {

    int value = comparison.relationB;
    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u = value;
    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l = value;
    if(existsInRelation(t[relations[comparison.relationA]]->columns[comparison.columnA], value, t[relations[comparison.relationA]]->size)){
        jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d = 1;
        jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f = (int) ((double)
                        jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f /
                                                                              jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d);
    }
    else {
        jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d = 0;
        jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f = 0;
    }

    for(int i=0; i<t[relations[comparison.relationA]]->columns_size; i++){

        if(i==comparison.columnA)
            continue;

        jes->jesbr[comparison.relationA].jesbc[i].d = (int) (
                jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d *
                calculateStatsDistinctsFactor(jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f,
                                                      jes->jesbr[comparison.relationA].jesbc[i].f,
                                                      jes->jesbr[comparison.relationA].jesbc[i].f,
                                                      jes->jesbr[comparison.relationA].jesbc[i].d));
        jes->jesbr[comparison.relationA].jesbc[i].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    }

    jes->results_num = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;
}

void executeLessThanFilter(JEStats *jes, Comparison comparison, table** t, int* relations) {

    int value = comparison.relationB;
    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d =
            (int) (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d *
                   ((double) (value - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l) /
                         (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l)));
    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f =
            (int) (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f *
                   ((double) (value - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l) /
                         (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l)));
    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u = value;

    for(int i=0; i<t[relations[comparison.relationA]]->columns_size; i++){

        if(i==comparison.columnA)
            continue;

        jes->jesbr[comparison.relationA].jesbc[i].d = (int) (
                jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d *
                calculateStatsDistinctsFactor(jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].d));
        jes->jesbr[comparison.relationA].jesbc[i].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    }

    jes->results_num = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;
}

void executeGreaterThanFilter(JEStats *jes, Comparison comparison, table** t, int* relations) {

    int value = comparison.relationB;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d =
            (int) (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d * ((double)
                        (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - value) /
                         (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l)));
    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f =
            (int) (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f * ((double)
                        (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - value) /
                         (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l)));
    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l = value;

    for(int i=0; i<t[relations[comparison.relationA]]->columns_size; i++){

        if(i==comparison.columnA)
            continue;

        jes->jesbr[comparison.relationA].jesbc[i].d = (int) (jes->jesbr[comparison.relationA].jesbc[i].d *
                                                             calculateStatsDistinctsFactor(jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].d));
        jes->jesbr[comparison.relationA].jesbc[i].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    }

    jes->results_num = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;
}

int existsInRelation(uint64_t *rel, int value, u_int64_t size) {

    for(int i=0; i<size; i++)
        if(rel[i] == value)
            return 1;

    return 0;

}

int existsInRelationInt(int *rel, int value, int size) {

    if(rel == NULL)
        return 0;

    for(int i=0; i<size; i++)
        if(rel[i] == value)
            return 1;

    return 0;

}

double calculateStatsDistinctsFactor(int f, int fA, int fC, int dC) {

    double val = 1 - (double) f / fA;
    val = pow(val, (double) fC / dC);
    val = 1 - val;
    return val;

}

JEStats *copyJEStats(JEStats *jes) {

    JEStats* result = malloc(sizeof(JEStats));

    result->relations_num = jes->relations_num;
    result->intermediateResultsNum = jes->intermediateResultsNum;
    result->intermediateResults = malloc(result->intermediateResultsNum * sizeof(int));
    result->results_num = jes->results_num;
    for(int i=0; i<result->intermediateResultsNum; i++)
        result->intermediateResults[i] = jes->intermediateResults[i];
    result->jesbr = malloc(result->relations_num * sizeof(JEStatsByRelation));

    for(int i=0; i<result->relations_num; i++){

        result->jesbr[i].columns_num = jes->jesbr[i].columns_num;
        result->jesbr[i].jesbc = malloc(result->jesbr[i].columns_num * sizeof(JEStatsByColumn));

        for(int j=0; j<result->jesbr[i].columns_num; j++){

            memcpy(&result->jesbr[i].jesbc[j], &jes->jesbr[i].jesbc[j], sizeof(JEStatsByColumn));

        }

    }

    return result;

}

void applyJoinToStats(JEStats *jes, int* arr, int arrLength, int newRel, Comparison_t *comparisons, table **t, int* relations) {

    for(int i=0; i<comparisons->comparisons_num; i++){

        int relA = comparisons->comparisons[i].relationA;
        int relB = comparisons->comparisons[i].relationB;
        int colA = comparisons->comparisons[i].columnA;
        int colB = comparisons->comparisons[i].columnB;

        if((existsInRelationInt(arr, comparisons->comparisons[i].relationA, arrLength) && newRel == comparisons->comparisons[i].relationB) ||
                (existsInRelationInt(arr, comparisons->comparisons[i].relationB, arrLength) && newRel == comparisons->comparisons[i].relationA)) {

            if (existsInRelationInt(jes->intermediateResults, comparisons->comparisons[i].relationA,
                                    jes->intermediateResultsNum) &&
                existsInRelationInt(jes->intermediateResults, comparisons->comparisons[i].relationB,
                                    jes->intermediateResultsNum)) {

                applyJoinToStatsSameRelation(jes, comparisons->comparisons[i], t, relations);

            }
            else {

                int *newIntermediateResults = malloc(
                        (arrLength + 1) * sizeof(int));        // Add newRel to the array of joined relations
                for (int j = 0; j < arrLength; j++)
                    newIntermediateResults[j] = jes->intermediateResults[j];
                newIntermediateResults[arrLength] = newRel;
                free(jes->intermediateResults);
                jes->intermediateResults = newIntermediateResults;
                jes->intermediateResultsNum += 1;

                jes->jesbr[relA].jesbc[colA].l =
                        jes->jesbr[relA].jesbc[colA].l > jes->jesbr[relB].jesbc[colB].l ?
                        jes->jesbr[relA].jesbc[colA].l :
                        jes->jesbr[relB].jesbc[colB].l;
                jes->jesbr[relB].jesbc[colB].l = jes->jesbr[relA].jesbc[colA].l;

                jes->jesbr[relA].jesbc[colA].u =
                        jes->jesbr[relA].jesbc[colA].u < jes->jesbr[relB].jesbc[colB].u ?
                        jes->jesbr[relA].jesbc[colA].u :
                        jes->jesbr[relB].jesbc[colB].u;
                jes->jesbr[relB].jesbc[colB].u = jes->jesbr[relA].jesbc[colA].u;

                int n = jes->jesbr[relA].jesbc[colA].u - jes->jesbr[relA].jesbc[colA].l + 1;

                jes->jesbr[relA].jesbc[colA].f = (jes->jesbr[relA].jesbc[colA].f * jes->jesbr[relB].jesbc[colB].f) / n;
                jes->jesbr[relB].jesbc[colB].f = jes->jesbr[relA].jesbc[colA].f;

                int dA = jes->jesbr[relA].jesbc[colA].d;
                int dB = jes->jesbr[relB].jesbc[colB].d;

                jes->jesbr[relA].jesbc[colA].d = (jes->jesbr[relA].jesbc[colA].d * jes->jesbr[relB].jesbc[colB].d) / n;
                jes->jesbr[relB].jesbc[colB].d = jes->jesbr[relA].jesbc[colA].d;

                for(int j=0; j<t[relations[relA]]->columns_size; j++){

                    if(j==colA)
                        continue;

                    jes->jesbr[relA].jesbc[i].d = (int) (jes->jesbr[relA].jesbc[i].d *
                                                         calculateStatsDistinctsFactor(jes->jesbr[relA].jesbc[colA].d,
                                                                                                                    dA,
                                                                                                                    jes->jesbr[relA].jesbc[i].f,
                                                                                                                    jes->jesbr[relA].jesbc[i].d));
                    jes->jesbr[relA].jesbc[i].f = jes->jesbr[relA].jesbc[colA].f;

                }

                for(int j=0; j<t[relations[relB]]->columns_size; j++){

                    if(j==colB)
                        continue;

                    jes->jesbr[relB].jesbc[i].d = (int) (jes->jesbr[relB].jesbc[i].d *
                                                         calculateStatsDistinctsFactor(jes->jesbr[relB].jesbc[colB].d,
                                                                                                    dB,
                                                                                                    jes->jesbr[relB].jesbc[i].f,
                                                                                                    jes->jesbr[relB].jesbc[i].d));
                    jes->jesbr[relB].jesbc[i].f = jes->jesbr[relB].jesbc[colB].f;

                }

            }

            jes->results_num = jes->jesbr[comparisons->comparisons[i].relationA].jesbc[comparisons->comparisons[i].columnA].f;
        }
    }

}

void applyJoinToStatsSameRelation(JEStats *jes, Comparison comparison, table **t, int* relations) {

    if(comparison.columnA == comparison.columnB){
        applyJoinToStatsSameColumn(jes, comparison, t, relations);
        return;
    }

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l =
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l > jes->jesbr[comparison.relationB].jesbc[comparison.columnB].l ?
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l :
            jes->jesbr[comparison.relationB].jesbc[comparison.columnB].l;
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].l = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u =
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u < jes->jesbr[comparison.relationB].jesbc[comparison.columnB].u ?
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u :
            jes->jesbr[comparison.relationB].jesbc[comparison.columnB].u;
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].u = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u;


    int n = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l + 1;
    int fA = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f / n;
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d = (int) (
            jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d *
            calculateStatsDistinctsFactor(jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f,
                                                                                                     fA,
                                                                                                     fA,
                                                                                                     jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d));
    jes->jesbr[comparison.relationB].jesbc[comparison.columnB].d = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d;

    for(int i=0; i<t[relations[comparison.relationA]]->columns_size; i++){

        if(i==comparison.columnA)
            continue;

        jes->jesbr[comparison.relationA].jesbc[i].d = (int) (
                jes->jesbr[comparison.relationA].jesbc[comparison.columnA].d *
                calculateStatsDistinctsFactor(jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationA].jesbc[i].d));
        jes->jesbr[comparison.relationA].jesbc[i].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    }

    for(int i=0; i<t[relations[comparison.relationB]]->columns_size; i++){

        if(i==comparison.columnB)
            continue;

        jes->jesbr[comparison.relationB].jesbc[i].d = (int) (
                jes->jesbr[comparison.relationB].jesbc[comparison.columnB].d *
                calculateStatsDistinctsFactor(jes->jesbr[comparison.relationB].jesbc[comparison.columnB].f,
                                                                                            jes->jesbr[comparison.relationB].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationB].jesbc[i].f,
                                                                                            jes->jesbr[comparison.relationB].jesbc[i].d));
        jes->jesbr[comparison.relationB].jesbc[i].f = jes->jesbr[comparison.relationB].jesbc[comparison.columnB].f;

    }

}

void applyJoinToStatsSameColumn(JEStats *jes, Comparison comparison, table **t, int* relations) {

    int n = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].u - jes->jesbr[comparison.relationA].jesbc[comparison.columnA].l + 1;

    jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f = (int)
            ((double) (jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f *
                        jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f) / n);

    for(int i=0; i<t[relations[comparison.relationA]]->columns_size; i++){

        if(i==comparison.columnA)
            continue;

        jes->jesbr[comparison.relationA].jesbc[i].f = jes->jesbr[comparison.relationA].jesbc[comparison.columnA].f;

    }

}

int *JoinEnumeration(table **t, Query *q) {

    int** graph = createGraph(q);		// Create the graph for the relations given in the comparisons.

    JEStats* jes = initializeJEStats(t, q);
    applyInitialFilters(jes, q, t);

    int** BestTree = malloc(factorial(q->query_relation_set->query_relations_num + 1) * sizeof(int*));		// Create the Best tree hash table.
    JEStats** Costs = malloc(factorial(q->query_relation_set->query_relations_num + 1) * sizeof(JEStats*));  // Create the Costs hash table.
    for(int i=0; i<factorial(q->query_relation_set->query_relations_num + 1); i++) {
        BestTree[i] = NULL;
        Costs[i] = NULL;
    }

    for(int i=0; i<q->query_relation_set->query_relations_num; i++){		// STEP 1 OF THE ALGORITHM
        int* a = malloc(sizeof(int));										// INITIALIZATION OF SINGLE ELEMENTS
        a[0] = i;
        BestTree[hashFunctionBestTree(a, 1)] = a;
        JEStats* stats = copyJEStats(jes);
        stats->intermediateResults[0] = i;	// The array of connected (joined) relations contains only the given one.
        stats->results_num = (int) t[q->query_relation_set->query_relations[i]]->size;
        Costs[hashFunctionBestTree(a, 1)] = stats;
    }

    freeJEStats(jes);

    int* array = createArray(q->query_relation_set->query_relations_num);	// Create an incrementing array of the relations

    for(int i=1; i<q->query_relation_set->query_relations_num; i++) {

        PermutationsQueue* pq = PQInit();		// Initialize the permutations Queue

        getCombination(array, q->query_relation_set->query_relations_num, i, pq);

        int* arr = PQPop(pq);

        while(arr != NULL){

            /*printf("Array:\t\t");
            for(int k=0; k<i; k++)
                printf("%d\t", arr[k]);
            printf("\n");*/

            for(int j=0; j<q->query_relation_set->query_relations_num; j++){

                if(existsInRelationInt(arr, array[j], i)
                   || !isConnected(graph, array[j], arr, i)
                   || BestTree[hashFunctionBestTree(arr, i)] == NULL)
                    continue;

                //printf("Relation %d is connected to above array, with index: %d\n", array[j], hashFunctionBestTree(arr, i));

                int index = hashFunctionBestTree(arr, i);

                int* newArr = malloc((i+1) * sizeof(int));
                for(int k=0; k<i; k++)
                    newArr[k] = arr[k];
                newArr[i] = array[j];

                /*printf("New Array:\t\t");
                for(int k=0; k<=i; k++)
                    printf("%d\t", newArr[k]);
                printf("\t\tIndex: %d\n", newIndex);*/
                int treeIndex = hashFunctionBestTree(newArr, i+1);

                JEStats* newStats = copyJEStats(Costs[index]);

                applyJoinToStats(newStats, arr, i, array[j], q->comparison_set, t, q->query_relation_set->query_relations);

                if(BestTree[treeIndex] == NULL || (Costs[treeIndex]->results_num > newStats->results_num)){

                    if(Costs[treeIndex] != NULL){

                        freeJEStats(Costs[treeIndex]);
                        free(BestTree[treeIndex]);

                    }

                    BestTree[treeIndex] = newArr;
                    Costs[treeIndex] = newStats;
                }
                else {
                    free(newArr);
                    freeJEStats(newStats);
                }
            }

            free(arr);

            arr = PQPop(pq);

        }

        PQDelete(pq);

    }

    free(array);

    int* connectedRelations = createConnectedRelationsFromGraph(q);
    int* bestTree = BestTree[hashFunctionBestTree(connectedRelations, connectedRelationsNum)];
/*
    printf("Best Tree:\t");
    for(int i=0; i<connectedRelationsNum; i++)
        printf("%d\t", bestTree[i]);
    printf("\n\n");*/

    int* comparisonsOrder = getComparisonsOrder(q->comparison_set, bestTree, q->query_relation_set->query_relations);

    free(connectedRelations);
    for(int i=0; i<factorial(q->query_relation_set->query_relations_num + 1); i++){
        if(BestTree[i] != NULL)
            free(BestTree[i]);
        if(Costs[i] != NULL)
            freeJEStats(Costs[i]);
    }

    for(int i=0; i<q->query_relation_set->query_relations_num; i++)
        free(graph[i]);
    free(graph);

    free(BestTree);
    free(Costs);

    return comparisonsOrder;

}

void freeJEStats(JEStats *jes) {

    for(int i=0; i<jes->relations_num; i++)
        free(jes->jesbr[i].jesbc);

    free(jes->intermediateResults);
    free(jes->jesbr);
    free(jes);

}

int *getComparisonsOrder(Comparison_t *comparisons, int *bestTree, int* relations) {

    int* result = NULL;
    int size = 0;

    for(int i=1; i<connectedRelationsNum; i++){

        for(int j=0; j<comparisons->comparisons_num; j++){

            if(comparisons->comparisons[j].action != JOIN)
                continue;

            if((existsInRelationInt(bestTree, comparisons->comparisons[j].relationA, i) && comparisons->comparisons[j].relationB == bestTree[i]) ||
                    (existsInRelationInt(bestTree, comparisons->comparisons[j].relationB, i) && comparisons->comparisons[j].relationA == bestTree[i])){

                size++;
                int* temp = malloc(size*sizeof(int));
                for(int k=0; k<(size-1); k++)
                    temp[k] = result[k];
                temp[size-1] = j;

                if(result != NULL)
                    free(result);

                result = temp;

            }
        }
    }
    joinComparisonsNum = size;

    return result;
}

int *createConnectedRelationsFromGraph(Query* q) {

    connectedRelationsNum = 0;
    int* result = NULL;

    for(int i=0; i<q->comparison_set->comparisons_num; i++){

        if(q->comparison_set->comparisons[i].action == JOIN && !existsInRelationInt(result, q->comparison_set->comparisons[i].relationA, connectedRelationsNum)){

            connectedRelationsNum++;
            int* temp = malloc(connectedRelationsNum*sizeof(int));
            for(int j=0; j<(connectedRelationsNum-1); j++)
                temp[j] = result[j];
            temp[connectedRelationsNum-1] = q->comparison_set->comparisons[i].relationA;
            if(result != NULL)
                free(result);
            result = temp;
        }
        if(q->comparison_set->comparisons[i].action == JOIN && !existsInRelationInt(result, q->comparison_set->comparisons[i].relationB, connectedRelationsNum)){

            connectedRelationsNum++;
            int* temp = malloc(connectedRelationsNum*sizeof(int));
            for(int j=0; j<(connectedRelationsNum-1); j++)
                temp[j] = result[j];
            temp[connectedRelationsNum-1] = q->comparison_set->comparisons[i].relationB;
            if(result != NULL)
                free(result);
            result = temp;
        }

    }

    return result;
}
