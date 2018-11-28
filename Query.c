#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Query.h"

Query* getQueries(){

    freopen("queries.txt","r",stdin);

    char* line = NULL;
    size_t length;
    int size;

    Query* queries = NULL;
    Query* temp;
    queries_num = 0;

    Query q;

    size = (int) getline(&line, &length, stdin);

    while(size > 0){

        q = getQueryFromLine(line, length);

        queries_num++;
        temp = malloc(queries_num*sizeof(Query));

        for(int i=0; i<(queries_num-1); i++){
            temp[i] = queries[i];
        }
        temp[queries_num-1] = q;

        if(queries != NULL){
            free(queries);
        }

        queries = malloc(queries_num* sizeof(Query));

        for(int i=0; i<queries_num; i++){
            queries[i] = temp[i];
        }

        free(temp);
        line = NULL;
        size = (int) getline(&line, &length, stdin);

    }

    free(line);

    return queries;

}


Query getQueryFromLine(char* line, size_t length){

    char* delimeters = "|";
    char* line_copy;
    char* num;

    char* relations;
    char* comparisons;
    char* columns;

    relations = malloc(length*sizeof(char));
    comparisons = malloc(length*sizeof(char));
    columns = malloc(length*sizeof(char));

    line_copy = malloc(sizeof(char)*length);
    strcpy(line_copy, line);

    num = strtok(line_copy, delimeters);
    strcpy(relations, num);

    num = strtok(NULL, delimeters);
    strcpy(comparisons, num);

    num = strtok(NULL, delimeters);
    strcpy(columns, num);

    Relation_t* rel = getRelations(relations);
    Comparison_t* comp = getComparisons(comparisons);
    Column_t* col = getColumns(columns);

    Query result;

    result.columns = col;
    result.relations = rel;
    result.comparisons = comp;

    free(line);
    free(line_copy);

    return result;
}

Relation_t *getRelations(char *line) {

    char* delimeters = " ";
    char* num;

    Relation_t* result = malloc(sizeof(Relation_t));
    result->relations_num = 0;

    char* l = malloc(strlen(line) + 1);
    strcpy(l, line);

    num = strtok(line, delimeters);

    while(num != NULL){

        result->relations_num++;
        num = strtok(NULL, delimeters);

    }

    result->relations = malloc(result->relations_num*sizeof(int));

    if(result->relations_num == 0){
        return result;
    }

    num = strtok(l, delimeters);
    int index = 0;

    while(num != NULL){

        result->relations[index] = atoi(num);
        index++;
        num = strtok(NULL, delimeters);

    }

    free(line);
    free(l);

    return result;
}

Comparison_t *getComparisons(char *line) {

    char* delimeters = "&";
    char* num;

    Comparison_t* result = malloc(sizeof(Comparison_t));
    result->comparisons_num = 0;

    char* l = malloc(strlen(line) + 1);
    strcpy(l, line);

    num = strtok(line, delimeters);

    while(num != NULL){

        result->comparisons_num++;
        num = strtok(NULL, delimeters);

    }

    result->comparisons = malloc(result->comparisons_num*sizeof(Comparison));

    if(result->comparisons_num == 0){
        return result;
    }

    char** comps = malloc(sizeof(char*)*result->comparisons_num);

    num = strtok(l, delimeters);
    int index = 0;

    while(num != NULL){

        comps[index] = malloc((strlen(num)+1)*sizeof(char));
        strcpy(comps[index], num);
        index++;
        num = strtok(NULL, delimeters);

    }

    for(int i=0; i<result->comparisons_num; i++){
        result->comparisons[i] = getComparisonFromQuery(comps[i]);
    }

    free(line);
    free(l);
    for(int i=0; i<result->comparisons_num; i++){
        free(comps[i]);
    }
    free(comps);

    return result;
}

Comparison getComparisonFromQuery(char *line) {

    Comparison result;

    char* delimeters = ".=<>";
    char* num;

    char* l = malloc(sizeof(char)*(strlen(line)+1));
    strcpy(l, line);

    for(int i=0; i<strlen(l); i++){

        if(l[i] == '=')
            result.action = 0;
        else if(l[i] == '<')
            result.action = 1;
        else if(l[i] == '>')
            result.action = 2;
        else{
            continue;
        }

        break;
    }

    num = strtok(l, delimeters);
    result.relationA = atoi(num);
    num = strtok(NULL, delimeters);
    result.columnA = atoi(num);
    num = strtok(NULL, delimeters);
    result.relationB = atoi(num);
    if(result.action == 0) {
        num = strtok(NULL, delimeters);
        result.columnB = atoi(num);
    }
    else
        result.columnB = -1;

    free(l);

    return result;
}


Column_t *getColumns(char *line) {

    char* delimeters = " ";
    char* num;

    Column_t* result = malloc(sizeof(Column_t));
    result->columns_num = 0;

    char* l = malloc(strlen(line) + 1);
    strcpy(l, line);
    l[strlen(line)] = '\0';

    num = strtok(line, delimeters);

    while(num != NULL){

        result->columns_num++;
        num = strtok(NULL, delimeters);

    }

    result->columns = malloc(result->columns_num*sizeof(Column));

    if(result->columns_num == 0){
        return result;
    }

    num = strtok(l, delimeters);
    int index = 0;

    while(num != NULL){

        result->columns[index] = getColumnsFromQuery(num);
        index++;
        num = strtok(NULL, delimeters);

    }

    free(line);
    free(l);

    return result;
}

Column getColumnsFromQuery(char *line) {

    Column result;

    char* rel = malloc(strlen(line));
    char* col = malloc(strlen(line));
    strcpy(rel, "0");
    strcpy(col, "0");

    int index = 0, rel_index;

    while(line[index] != '.'){

        rel[index] = line[index];
        index++;

    }

    index++;
    rel_index = index;
    index = 0;

    while(line[index + rel_index] != '\0'){

        col[index] = line[index+rel_index];
        index++;

    }

    result.relation = atoi(rel);
    result.column = atoi(col);

    free(rel);
    free(col);

    return result;

}

void freeQueries(Query *queries) {

    for(int i=0; i<queries_num; i++) {

        free(queries[i].columns->columns);
        free(queries[i].columns);
        free(queries[i].comparisons->comparisons);
        free(queries[i].comparisons);
        free(queries[i].relations->relations);
        free(queries[i].relations);

    }

    free(queries);
}
