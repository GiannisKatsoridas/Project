#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Query.h"

Query* getQueries(){

    char* line = NULL;
    size_t length;
    int size;

    Query* queries = NULL;
    Query* temp;
    queries_num = 0;

    Query q;

    size = (int) getline(&line, &length, stdin);

    if(size < 0){
        free(line);
        return NULL;
    }

        
    while(strncmp(line, "F", strlen("F"))){

        if(line[strlen(line)-1]=='\n'){
            line[strlen(line) - 1] = '\0';
        }
        
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

    char* query_relations;
    char* comparisons;
    char* columns;

    query_relations = malloc(length*sizeof(char));
    comparisons = malloc(length*sizeof(char));
    columns = malloc(length*sizeof(char));

    line_copy = malloc(sizeof(char)*length);
    strcpy(line_copy, line);

    num = strtok(line_copy, delimeters);
    strcpy(query_relations, num);

    num = strtok(NULL, delimeters);
    strcpy(comparisons, num);

    num = strtok(NULL, delimeters);
    strcpy(columns, num);

    Query_Relation_t* rel = getQueryRelations(query_relations);
    Comparison_t* comp = getComparisons(comparisons);
    Column_t* col = getColumns(columns);

    Query result;

    result.column_set = col;
    result.query_relation_set = rel;
    result.comparison_set = comp;

    free(line);
    free(line_copy);

    return result;
}

Query_Relation_t *getQueryRelations(char *line) {

    char* delimeters = " ";
    char* num;

    Query_Relation_t* result = malloc(sizeof(Query_Relation_t));
    result->query_relations_num = 0;

    char* line_copy = malloc(strlen(line) + 1);
    strcpy(line_copy, line);

    num = strtok(line, delimeters);

    while(num != NULL){

        result->query_relations_num++;
        num = strtok(NULL, delimeters);

    }

    result->query_relations = malloc(result->query_relations_num*sizeof(int));

    if(result->query_relations_num == 0){
        return result;
    }

    num = strtok(line_copy, delimeters);
    int index = 0;

    while(num != NULL){

        result->query_relations[index] = atoi(num);
        index++;
        num = strtok(NULL, delimeters);

    }

    free(line);
    free(line_copy);

    return result;
}

Comparison_t *getComparisons(char *line) {

    char* delimeters = "&";
    char* num;

    Comparison_t* result = malloc(sizeof(Comparison_t));
    result->comparisons_num = 0;

    char* line_copy = malloc(strlen(line) + 1);
    strcpy(line_copy, line);

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

    num = strtok(line_copy, delimeters);
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
    free(line_copy);
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

    char* line_copy = malloc(sizeof(char)*(strlen(line)+1));
    strcpy(line_copy, line);

    for(int i=0; i<strlen(line_copy); i++){

        if(line_copy[i] == '=')
            result.action = EQUAL;
        else if(line_copy[i] == '<')
            result.action = LESS_THAN;
        else if(line_copy[i] == '>')
            result.action = GREATER_THAN;
        else{
            continue;
        }

        break;
    }

    num = strtok(line_copy, delimeters);
    result.relationA = atoi(num);
    num = strtok(NULL, delimeters);
    result.columnA = atoi(num);
    num = strtok(NULL, delimeters);
    result.relationB = atoi(num);
    if(result.action == EQUAL) {
        num = strtok(NULL, delimeters);
        if(num != NULL)
        {
            result.columnB = atoi(num);
            result.action = JOIN;
        }
        else
            result.columnB = -1;
    }
    else
        result.columnB = -1;

    free(line_copy);

    return result;
}


Column_t *getColumns(char *line) {

    char* delimeters = " ";
    char* num;

    Column_t* result = malloc(sizeof(Column_t));
    result->columns_num = 0;

    char* line_copy = malloc(strlen(line) + 1);
    strcpy(line_copy, line);
    line_copy[strlen(line)] = '\0';

    num = strtok(line, delimeters);

    while(num != NULL){

        result->columns_num++;
        num = strtok(NULL, delimeters);

    }

    result->columns = malloc(result->columns_num*sizeof(Column));

    if(result->columns_num == 0){
        return result;
    }

    num = strtok(line_copy, delimeters);
    int index = 0;

    while(num != NULL){

        result->columns[index] = getColumnsFromQuery(num);
        index++;
        num = strtok(NULL, delimeters);

    }

    free(line);
    free(line_copy);

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

        free(queries[i].column_set->columns);
        free(queries[i].column_set);
        free(queries[i].comparison_set->comparisons);
        free(queries[i].comparison_set);
        free(queries[i].query_relation_set->query_relations);
        free(queries[i].query_relation_set);

    }

    free(queries);
}
